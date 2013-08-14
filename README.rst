jsonquery
========================================================

Basic json -> sqlalchemy query builder


Installation
========================================================

::

    pip install jsonquery

Basic Usage
========================================================

Let's define a model and get an engine set up::

    from sqlalchemy import Column, Integer, String, create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'users'
        id = Column(Integer, primary_key=True)
        name = Column(String)
        email = Column(String)
        age = Column(Integer)
        height = Column(Integer)
    engine = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(engine)
    model = User
    session = sessionmaker(bind=engine)()

We want to get all users whose name starts with 'Pat' and are
at least 21::

    from jsonquery import jsonquery

    json = {
        "operator": "and",
        "value": [
            {
                "operator": ">=",
                "column": "age",
                "value": 21
            },
            {
                "operator": "ilike",
                "column": "name",
                "value": "pat%"
            }
        ]
    }

    query = jsonquery(session, User, json)
    users = query.all()

Supported Data Types
========================================================

jsonquery doesn't care about column type.  Instead, it uses a whitelist of operators,
where keys are strings (the same that would be passed in the "operator" field of a node)
and the values are functions that take a column object and a value and return a
sqlalchemy criterion.  Here are some examples::

    def greater_than(column, value):
        return column > value
    register_operator(">", greater_than)

    def like(column, value):
        like_func = getattr(column, 'like')
        return like_func(value)
    register_operator("like", like)

By default, the following are registered::

    >, >=, ==, !=, <=, <
    like, ilike, in_

Use ``unregister_operator(opstring)`` to remove an operator.

Future Goals
========================================================

There are a few features I want to add, but these are mostly convenience and aren't necessary to
the core application, which I believe is satisfied.

Compressed and/or format
--------------------------------------------------------

Reduce repetitive column and operator specification when possible by allowing non-scalar values
for column operators.  By flipping the nesting restriction on logical operators, we can omit
fields specified at the column level.  This is especially prominent in string matching,
when the column and operator are the same, but we want to compare against 3+ values.

Currently::

    {
        "operator": "or",
        "value": [
            {
                "column": "age",
                "operator": "<=",
                "value": 16
            },
            {
                "column": "age",
                "operator": ">=",
                "value": 21
            },
            {
                "column": "age",
                "operator": "==",
                "value": 18
            }
        ]
    }

With compressed logical operators::

    {
        "column": "age"
        "value": {
            "operator": "or",
            "value": [
                {
                    "operator": "<=",
                    "value": 16
                },
                {
                    "operator": ">=",
                    "value": 21
                },
                {
                    "operator": "==",
                    "value": 18
                }
            ]
        }
    }

Or, when the operator is the same::

    {
        "column": "name"
        "operator": "like"
        "value": {
            "operator": "or",
            "value": [
                "Bill",
                "Mary",
                "Steve"
            ]
        }
    }

Motivation
========================================================

I want to build complex sql queries from a request body, and json is a nice way
to specify nested queries.  As far as security is concerned, column/value names are passed
into a set of functions which is hardcoded, and is primarily either attribute lookup
(string functions like, ilike) or standard mathematical operators (operator.gt, for instance).
