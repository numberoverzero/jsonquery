import operator
import collections
import sqlalchemy


class InterrogateException(Exception):
    pass


DEFAULT_QUERY_CONSTRAINTS = {
    'breadth': None,
    'depth': None,
    'elements': 64
}

NUMERIC_OPERATORS = {
    '<': operator.lt,
    '<=': operator.le,
    '!=': operator.ne,
    '==': operator.eq,
    '>=': operator.ge,
    '>': operator.gt,
}

CASE_OPERATORS = {
    'strict': 'like',
    'ignore': 'ilike',
}

STRING_MATCH_FUNCS = {
    'match-prefix': lambda s: s + '%',    #  "foo" matches "foobar"
    'match-suffix': lambda s: '%' + s,    #  "foo" matches "myfoo"
    'match-any': lambda s: '%' + s + '%', #  "foo" matches "myfoobar"
    'match-strict': lambda s: s,          #  "foo" matches only "foo"
}

_type_constraints = 'string', 'numeric', 'nullable'


def _is_real_sequence(obj):
    return isinstance(obj, collections.Sequence) and not isinstance(obj, basestring)


class Builder(object):
    '''
    Takes a model and set of constraints, and builds sqlalchemy queries from json.
    '''

    def __init__(self, model, type_constraints, query_constraints=None):
        '''
        model:              SQLAlchemy model to perform queries on

        type_constraints:   Dictionary of string -> list, whose keys are:
                                'string', 'numeric', 'nullable'
                            For string and numeric, the lists are all column names that must be of that type on input.
                            nullable is a list of column names that can be null on input - by default, column names
                            listed in string or numeric assume they cannot take null values.
                            Including the same column in both string and numeric columns is invalid.

                            Example:
                            type_constraints = {
                                'string': [
                                    'name',
                                    'address',
                                    'email',
                                    'phone'
                                ],

                                'numeric': [
                                    'age',
                                    'pets',
                                    'followers',
                                    'following'
                                ],

                                'nullable': [
                                    'address',
                                    'phone',
                                    'followers'
                                ]
                            }

        query_constraints: (Optional) Dictonary of string -> integer, whose keys are:
                                'breadth', 'depth', 'elements'
                            (All keys are optional) Each is a constraint on the query shape - if breadth is 5 and
                            there is an 'or' node with an array of 12 column filters, the query will be rejected.
                            depth refers to nesting, and elements is the total number of query elements. Logical
                            operators (and, or, not) also count as elements, so 'and': ['foo': 'bar'] represents
                            two elements. Falsey values (None, 0) indicate no upper limit.

                            Example:
                            query_constraints = {
                                'breadth': None,
                                'depth': 32,
                                'elements': 64
                            }
        '''
        self.model = model
        self.type_constraints = type_constraints
        self.query_constraints = dict(DEFAULT_QUERY_CONSTRAINTS)
        if query_constraints:
            self.query_constraints.update(query_constraints)

    def build(self, json):
        '''
        Builds a query from the given json.

        Objects:
            Logical Operators
                {
                    operator: 'and',
                    value: [
                        OBJ1,
                        OBJ2,
                        ...
                        OBJN
                    ]
                }

            Subqueries: Numeric
                {
                    column: 'age',
                    operator: '>=',
                    value: 18
                }

            Subqueries: Strings
                {
                    column: 'name',
                    operator: 'match-prefix',
                    case: 'ignore',
                    value: 'pat'
                }

        Logical operators 'and' and 'or' take an array of values, while 'not' takes a single value.

        It is invalid to have a logical operator as the value of a subquery.

        Numeric operators are:
            <, <=, ==, !=, >=, >

        String operators are:
            match-prefix    "foo" matches "foobar"
            match-suffix    "foo" matches "myfoo"
            match-any       "foo" matches "myfoobar"
            match-strict    "foo" matches only "foo"

        String case values are:
            ignore          "foo" matches "FOOBAR_CONSTANT"
            strict          "foo" does not match "FOOBAR"

        '''

        count = depth = 0
        query, total_elements = self._build(json, count, depth)
        return self.model.query.filter(query)

    def _build(self, node, count, depth):
        '''
        Delegate the build call based on key, comparing against self.operators and self.type_constraints
        Do not validate, or increment depth/count since the called builder will handle that.
        '''
        logical_operators = {
            'and': (self._build_sql_sequence, sqlalchemy.and_),
            'or': (self._build_sql_sequence, sqlalchemy.or_),
            'not': (self._build_sql_unary, sqlalchemy.not_),
        }
        op = node['operator']
        if op in logical_operators:
            builder, func = logical_operators[op]
            return builder(node, count, depth, func)
        else:
            return self._build_column(node, count, depth)

    def _build_sql_sequence(self, node, count, depth, func):
        '''
        func is either sqlalchemy.and_ or sqlalchemy.or_
        Build each subquery in node['value'], then combine with func(*subqueries)
        '''
        count += 1
        depth += 1
        subqueries = []
        for value in node['value']:
            subquery, count = self._build(value, count, depth)
            subqueries.append(subquery)
        return func(*subqueries), count

    def _build_sql_unary(self, node, count, depth, func):
        '''
        func is sqlalchemy.not_ (may support others)
        '''
        count += 1
        depth += 1
        value = node['value']
        self._validate_query_constraints(value, count, depth)
        if _is_real_sequence(value):
            raise TypeError("Cannot compare a column to a sequence".format(node, value))

        subquery, count = self._build(node, count, depth)
        return func(subquery), count

    def _build_column(self, node, count, depth):
        '''
        Delegate the call based on type
        Do not validate, or increment depth/count since the called builder will handle that.
        '''
        column = node['column']
        if column in self.type_constraints['string']:
            build = lambda n, c, d: self._build_column_string(n, c, d)
        elif column in self.type_constraints['numeric']:
            build = lambda n, c, d: self._build_column_numeric(n, c, d)
        return build(node, count, depth)

    def _build_column_string(self, node, count, depth):
        count += 1
        depth += 1
        operator = node['operator']
        case = node['case']
        column = node['column']
        value = node['value']
        self._validate_query_constraints(value, count, depth)
        self._validate_nullable(value, column)

        col = getattr(self.model, column, None)  # MyModel.my_column
        case_func = CASE_OPERATORS[case]
        op = getattr(col, case_func)  # MyModel.my_column.ilike

        # Compute search string based on operator
        search_func = STRING_MATCH_FUNCS[operator]
        search = search_func(value)
        return op(search), count


    def _build_column_numeric(self, node, count, depth):
        count += 1
        depth += 1

        operator = node['operator']
        column = node['column']
        value = node['value']
        self._validate_query_constraints(value, count, depth)
        self._validate_nullable(column, value)

        op = NUMERIC_OPERATORS[operator]
        col = getattr(self.model, column)
        return op(col, value), count

    def _validate_query_constraints(self, value, count, depth):
        '''Raises if any query constraints are violated'''
        max_breadth = self.query_constraints['breadth']
        max_depth = self.query_constraints['depth']
        max_elements = self.query_constraints['elements']

        if max_depth and depth > max_depth:
            raise InterrogateException('Depth limit ({}) exceeded'.format(max_depth))

        element_breadth = 0
        if _is_real_sequence(value):
            element_breadth = len(value)
        
        if max_breadth and element_breadth > max_breadth:
                raise InterrogateException('Breadth limit ({}) exceeded'.format(max_breadth))
        
        count += (element_breadth + 1)
        if max_elements and count > max_elements:
            raise InterrogateException('Filter elements limit ({}) exceeded'.format(max_elements))

    def _validate_nullable(self, column, value):
        nullable = column in self.type_constraints['nullable']
        if value is None and not nullable:
            raise ValueError('Column "{}" cannot be null.'.format(column))
