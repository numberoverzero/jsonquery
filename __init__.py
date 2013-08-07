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

INTEGER_OPERATORS = {
    '<': operator.lt,
    '<=': operator.le,
    '!=': operator.ne,
    '==': operator.eq,
    '>=': operator.ge,
    '>': operator.gt,
}

STRING_OPERATORS = [
    'like',
    'ilike'
]


class Builder(object):
    '''
    Takes a model and set of constraints, and builds sqlalchemy queries from json.
    '''

    def __init__(self, model, query_constraints=None):
        '''
        model:              SQLAlchemy model to perform queries on

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
                    operator: 'ilike',
                    value: 'pat%'
                }

        Logical operators 'and' and 'or' take an array of values, while 'not' takes a single value.
        It is invalid to have a logical operator as the value of a subquery.

        Numeric operators are:
            <, <=, ==, !=, >=, >
        String operators are:
            like    case-sensitive match
            ilike   case-insensitive match

            String wildcard character is % (so "pat%" matches "patrick" and "patty")
            with default escape character '/'
        '''

        count = depth = 0
        query, total_elements = self._build(json, count, depth)
        return self.model.query.filter(query)

    def _build(self, node, count, depth):
        '''
        Delegate the build call based on node operator, comparing against logical operators
        '''
        count += 1
        depth += 1
        value = node['value']
        self._validate_query_constraints(value, count, depth)
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
        subqueries = []
        for value in node['value']:
            subquery, count = self._build(value, count, depth)
            subqueries.append(subquery)
        return func(*subqueries), count

    def _build_sql_unary(self, node, count, depth, func):
        '''
        func is sqlalchemy.not_ (may support others)
        '''
        subquery, count = self._build(node, count, depth)
        return func(subquery), count

    def _build_column(self, node, count, depth):
        '''
        Delegate the call based on type
        '''
        # string => sqlalchemy.orm.attributes.InstrumentedAttribute
        column = node['column']
        column = getattr(self.model, column)

        op = node['operator']
        value = node['value']

        builders = {
            sqlalchemy.types.String: self._build_col_string,
            sqlalchemy.types.Integer: self._build_col_integer
        }
        build = builders[column.type]

        return build(column, op, value), count

    def _build_col_string(self, col, op, value):
        func = getattr(col, op)
        return func(value)


    def _build_col_integer(self, col, op, value):
        func = INTEGER_OPERATORS[op]
        return func(col, value)

    def _validate_query_constraints(self, value, count, depth):
        '''Raises if any query constraints are violated'''
        max_breadth = self.query_constraints['breadth']
        max_depth = self.query_constraints['depth']
        max_elements = self.query_constraints['elements']

        if max_depth and depth > max_depth:
            raise InterrogateException('Depth limit ({}) exceeded'.format(max_depth))

        element_breadth = 1
        if isinstance(value, collections.Sequence) and not isinstance(value, basestring):
            element_breadth = len(value)

        if max_breadth and element_breadth > max_breadth:
                raise InterrogateException('Breadth limit ({}) exceeded'.format(max_breadth))

        count += element_breadth
        if max_elements and count > max_elements:
            raise InterrogateException('Filter elements limit ({}) exceeded'.format(max_elements))
