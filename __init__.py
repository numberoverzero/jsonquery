import operator
import collections
import sqlalchemy


class InterrogateException(Exception):
    pass


DEFAULT_QUERY_CONSTRAINTS = {
    u'breadth': None,
    u'depth': None,
    u'elements': 64
}

NUMERIC_OPERATORS = {
    u'<': operator.lt,
    u'<=': operator.le,
    u'!=': operator.ne,
    u'==': operator.eq,
    u'>=': operator.ge,
    u'>': operator.gt,
}

CASE_OPERATORS = {
    u'strict': 'like',
    u'ignore': 'ilike',
}

STRING_MATCH_FUNCS = {
    u'match-prefix': lambda s: _unicodify(s + '%'),     #  "foo" matches "foobar"
    u'match-suffix': lambda s: _unicodeify('%' + s),    #  "foo" matches "myfoo"
    u'match-any': lambda s: _unicodify('%' + s + '%'),  #  "foo" matches "myfoobar"
    u'match-strict': lambda s: s,                       #  "foo" matches only "foo"
}

_operators = {u'and', u'or', u'not'}
_type_constraints = {u'string', u'numeric', u'nullable'}


def _is_real_sequence(obj):
    return isinstance(obj, collections.Sequence) and not isinstance(obj, basestring)


def _unicodify(value):
    if _is_real_sequence(value):
        return [_unicodify(x) for x in value]
    return unicode(value)


class Builder(object):
    '''
    Takes a model and set of constraints, and builds sqlalchemy queries from json.
    '''

    def __init__(self, model, operators, type_constraints, query_constraints=None):
        '''
        model:              SQLAlchemy model to perform queries on

        operators:          Dictionary of string -> list, whose keys are 'and', 'or', and 'not'.
                            Each value is a list of strings which are allowed for that operator.
                            Matches are CASE SENSITIVE.

                            Example:
                            operators = {
                                'and': ['AND', '&&', '+'],
                                'or': ['OR', '||'],
                                'not': ['NOT', '~', '!']
                            }

        type_constraints:   Dictionary of string -> list, whose keys are:
                                'string', 'numeric', 'nullable'
                            For string and numeric, the lists are all column names that must be of that type on input.
                            nullable is a list of column names that can be null on input - by default, column names
                            listed in string or numeric assume they cannot take null values, and passing null is
                            invalid. Any input that contains column names not listed in string or numeric is invalid.
                            It is invalid for a column name to have more than one type constraint.

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
                            there is an OR node with an array of 12 column filters, the query will be rejected.
                            depth refers to nesting, and elements is the total number of query elements. Logical
                            operators (AND, OR, NOT) also count as elements, so 'AND': ['foo': 'bar'] represents
                            two elements. Falsey values (None, 0) indicate no upper limit.

                            Example:
                            query_constraints = {
                                'breadth': None,
                                'depth': 32,
                                'elements': 64
                            }
        '''
        self.model = model

        if self.model is None:
            raise ValueError(u"Must provide a valid model.")

        self.operators = dict(operators)
        operator_keys = set(self.operators.keys())
        if not operator_keys.issuperset(_operators):
            # Missing at least one operator
            raise ValueError(u"Must specifiy aliases for all three logical operators.")
        for operator in _operators:
            operators = self.operators[operator]
            if _is_real_sequence(operators):
                self.operators[operator] = list(operators)
            else:
                self.operators[operator] = [operators]

        self.type_constraints = {}
        for type_c in _type_constraints:
            columns = _unicodify(type_constraints[type_c])
            if not _is_real_sequence(columns):
                columns = [columns]
            self.type_constraints[type_c] = columns
        string_c = set(self.type_constraints[u'string'])
        numeric_c = set(self.type_constraints[u'numeric'])
        duplicate_constraints = string_c.intersection(numeric_c)
        if duplicate_constraints:
            # Found an element in both sets
            raise ValueError(u"Specified two type constraints for the following: ".format(duplicate_constraints))

        query_constraints = query_constraints or {}
        self.query_constraints = dict(DEFAULT_QUERY_CONSTRAINTS)
        self.query_constraints.update(query_constraints)

    @property
    def max_breadth(self):
        return self.query_constraints[u'breadth']

    @property
    def max_depth(self):
        return self.query_constraints[u'depth']

    @property
    def max_elements(self):
        return self.query_constraints[u'elements']

    def build(self, json):
        '''
        Builds a query from the given json.

        Objects:
            Logical Operators
                {
                    operator: 'AND',
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

        Logical operators AND and OR take an array of values, while NOT takes a single value.

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
        operator = node[u'operator']
        if operator in self.operators[u'and']:
            build = lambda n, c, d: self._build_sql_sequence(n, c, d, sqlalchemy.and_)
        elif operator in self.operators[u'or']:
            build = lambda n, c, d: self._build_sql_sequence(n, c, d, sqlalchemy.or_)
        elif operator in self.operators[u'not']:
            build = lambda n, c, d: self._build_sql_unary(n, c, d, sqlalchemy.not_)
        else:
            # Simple column filter
            build = lambda n, c, d: self._build_column(n, c, d)

        return build(node, count, depth)

    def _build_sql_sequence(self, node, count, depth, func):
        '''
        func is either sqlalchemy.and_ or sqlalchemy.or_
        Build each subquery in node['value'], then combine with func(*subqueries)
        '''
        count += 1
        depth += 1
        subqueries = []
        for value in node[u'value']:
            subquery, count = self._build(value, count, depth)
            subqueries.append(subquery)
        return func(*subqueries), count

    def _build_sql_unary(self, node, count, depth, func):
        '''
        func is sqlalchemy.not_ (may support others)
        '''
        count += 1
        depth += 1
        value = node[u'value']
        self._validate_query_constraints(value, count, depth)
        if _is_real_sequence(value):
            raise TypeError(u"Cannot compare a column to a sequence".format(node, value))

        subquery, count = self._build(node, count, depth)
        return func(subquery), count

    def _build_column(self, node, count, depth):
        '''
        Delegate the call based on type
        Do not validate, or increment depth/count since the called builder will handle that.
        '''
        column = node[u'column']
        if column in self.type_constraints[u'string']:
            build = lambda n, c, d: self._build_column_string(n, c, d)
        elif column in self.type_constraints[u'numeric']:
            build = lambda n, c, d: self._build_column_numeric(n, c, d)
        return build(node, count, depth)

    def _build_column_string(self, node, count, depth):
        count += 1
        depth += 1
        operator = node[u'operator']
        case = node[u'case']
        column = node[u'column']
        value = node[u'value']
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

        operator = node[u'operator']
        column = node[u'column']
        value = node[u'value']
        self._validate_query_constraints(value, count, depth)
        self._validate_nullable(column, value)

        op = NUMERIC_OPERATORS[operator]
        col = getattr(self.model, column)
        return op(col, value), count

    def _validate_query_constraints(self, value, count, depth):
        '''Raises if any query constraints are violated'''
        depth += 1
        if self.max_depth and depth > self.max_depth:
            raise InterrogateException(u'Depth limit ({}) exceeded'.format(self.max_depth))

        if _is_real_sequence(value):
            element_breadth = len(value)
            if self.max_breadth and element_breadth > self.max_breadth:
                raise InterrogateException(u'Breadth limit ({}) exceeded'.format(self.max_breadth))
            count += len(value)

        count += 1
        if self.max_elements and count > self.max_elements:
            raise InterrogateException(u'Filter elements limit ({}) exceeded'.format(self.max_elements))

    def _validate_nullable(self, column, value):
        nullable = column in self.type_constraints[u'nullable']
        if value is None and not nullable:
            raise ValueError(u'Column "{}" cannot be null.'.format(column))
