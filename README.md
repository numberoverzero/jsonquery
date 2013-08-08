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

Presently, jsonquery supports Integers and Strings.  More to come in the immediate future!

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

Make column operators pluggable
--------------------------------------------------------

Right now String and Integer operators are hardcoded, and there's no good way
to say "here's the function to use when the column is my custom FooType".  Besides
supporting all of the build-in sqlalchemy column types, it must be possible to easily
integrate custom columns, prefereably allowing re-use of existing functions
(so that users don't need to import operator and refer to operator.gt all over).

Add unobtrusive column/operator white-listing
--------------------------------------------------------

This was in the first version, but I cut it because I think a json parser could handle that
validation better.  It may be better to keep this component slim, and let users make their own
filtering step using parsers.

Motivation
========================================================

I want to build complex sql queries from a request body, and json is a nice way
to specify nested queries.  As far as security is concerned, column/value names are passed
into a set of functions which is hardcoded, and is primarily either attribute lookup
(string functions like, ilike) or standard mathematical operators (operator.gt, for instance).
