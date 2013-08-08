import json
import unittest
from sqlalchemy import Column, Integer, String, create_engine, and_, or_, not_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from jsonquery import jsonquery


def jsonify(dict):
    # Easy validation that the test data isn't invalid json
    return json.loads(json.dumps(dict))


class JsonQueryTestCase(unittest.TestCase):

    def setUp(self):
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
        self.model = User
        self.session = sessionmaker(bind=engine)()

    def tearDown(self):
        self.session.close()

    def add_user(self, **kwargs):
        user = self.model(**kwargs)
        self.session.add(user)
        self.session.commit()

    @property
    def query(self):
        return self.session.query(self.model)

    def test_basic_query(self):
        self.add_user(age=10)

        json = jsonify({
            'column': 'age',
            'value': 10,
            'operator': '=='
        })
        actual_user = jsonquery(self.session, self.model, json).one()

        expected_user = self.query.filter(self.model.age == 10).one()
        assert actual_user is expected_user

    def test_element_limit(self):
        self.add_user(age=10)

        json = jsonify({
            'operator': 'and',
            'value': [
                {
                    'column': 'age',
                    'value': 10,
                    'operator': '=='
                }
            ]
        })

        with self.assertRaises(ValueError):
            jsonquery(self.session, self.model, json, max_elements=1).one()

    def test_depth_limit(self):
        self.add_user(age=10)

        json = jsonify({
            'operator': 'and',
            'value': [
                {
                    'column': 'age',
                    'value': 10,
                    'operator': '=='
                }
            ]
        })

        with self.assertRaises(ValueError):
            jsonquery(self.session, self.model, json, max_depth=1).one()

    def test_breadth_limit(self):
        self.add_user(age=10)

        json = jsonify({
            'operator': 'and',
            'value': [
                {
                    'column': 'age',
                    'value': 10,
                    'operator': '=='
                },
                {
                    'column': 'age',
                    'value': 10,
                    'operator': '=='
                }
            ]
        })

        with self.assertRaises(ValueError):
            jsonquery(self.session, self.model, json, max_breadth=1).one()

    def test_basic_and(self):
        self.add_user(age=10)

        json = jsonify({
            'operator': 'and',
            'value': [
                {
                    'column': 'age',
                    'value': 10,
                    'operator': '=='
                }
            ]
        })
        actual_user = jsonquery(self.session, self.model, json).one()
        expected_user = self.query.filter(and_(self.model.age == 10)).one()
        assert actual_user is expected_user

    def test_multi_criteria_and(self):
        self.add_user(age=10, height=20)
        self.add_user(age=10, height=15)
        self.add_user(age=5, height=15)

        json = jsonify({
            'operator': 'and',
            'value': [
                {
                    'column': 'age',
                    'value': 10,
                    'operator': '=='
                },
                {
                    'column': 'height',
                    'value': 15,
                    'operator': '=='
                }
            ]
        })
        actual_user = jsonquery(self.session, self.model, json).one()
        expected_user = self.query.filter(and_(self.model.age == 10, self.model.height == 15)).one()
        assert actual_user is expected_user

    def test_basic_or(self):
        self.add_user(age=10)

        json = jsonify({
            'operator': 'or',
            'value': [
                {
                    'column': 'age',
                    'value': 10,
                    'operator': '=='
                }
            ]
        })
        actual_user = jsonquery(self.session, self.model, json).one()
        expected_user = self.query.filter(or_(self.model.age == 10)).one()
        assert actual_user is expected_user

    def test_multi_criteria_or(self):
        self.add_user(age=10, height=20)
        self.add_user(age=10, height=15)
        self.add_user(age=5, height=15)

        json = jsonify({
            'operator': 'or',
            'value': [
                {
                    'column': 'age',
                    'value': 10,
                    'operator': '=='
                },
                {
                    'column': 'height',
                    'value': 15,
                    'operator': '=='
                }
            ]
        })
        actual_users = jsonquery(self.session, self.model, json).all()
        expected_users = self.query.filter(or_(self.model.age == 10, self.model.height == 15)).all()
        assert 3 == len(actual_users) == len(expected_users)
        assert set(actual_users) == set(expected_users)

    def test_basic_not(self):
        self.add_user(age=10)
        self.add_user(age=20)
        self.add_user(age=30)

        json = jsonify({
            'operator': 'not',
            'value':
            {
                'column': 'age',
                'value': 10,
                'operator': '=='
            }
        })

        actual_users = jsonquery(self.session, self.model, json).all()
        expected_users = self.query.filter(not_(self.model.age == 10)).all()
        assert 2 == len(actual_users) == len(expected_users)
        assert set(actual_users) == set(expected_users)


class IntegerColumnTestCase(unittest.TestCase):

    def setUp(self):
        Base = declarative_base()

        class Foo(Base):
            __tablename__ = 'foos'
            id = Column(Integer, primary_key=True)
            foo = Column(Integer)
        engine = create_engine("sqlite://", echo=True)
        Base.metadata.create_all(engine)
        self.model = Foo
        self.session = sessionmaker(bind=engine)()

        self.add_foo(10)
        self.add_foo(15)
        self.add_foo(20)

    def tearDown(self):
        self.session.close()

    def add_foo(self, value):
        foo = self.model(foo=value)
        self.session.add(foo)
        self.session.commit()

    @property
    def query(self):
        return self.session.query(self.model)

    def test_eq(self):
        json = jsonify({
            'column': 'foo',
            'value': 10,
            'operator': '=='
        })
        actual_foo = jsonquery(self.session, self.model, json).one()
        expected_foo = self.query.filter(self.model.foo == 10).one()
        assert actual_foo is expected_foo

    def test_ne(self):
        json = jsonify({
            'column': 'foo',
            'value': 10,
            'operator': '!='
        })
        actual_foos = jsonquery(self.session, self.model, json).all()
        expected_foos = self.query.filter(self.model.foo != 10).all()
        assert 2 == len(actual_foos) == len(expected_foos)
        assert set(actual_foos) == set(expected_foos)

    def test_lt(self):
        json = jsonify({
            'column': 'foo',
            'value': 15,
            'operator': '<'
        })
        actual_foo = jsonquery(self.session, self.model, json).one()
        expected_foo = self.query.filter(self.model.foo < 15).one()
        assert actual_foo is expected_foo

    def test_le(self):
        json = jsonify({
            'column': 'foo',
            'value': 15,
            'operator': '<='
        })
        actual_foos = jsonquery(self.session, self.model, json).all()
        expected_foos = self.query.filter(self.model.foo <= 15).all()
        assert 2 == len(actual_foos) == len(expected_foos)
        assert set(actual_foos) == set(expected_foos)

    def test_gt(self):
        json = jsonify({
            'column': 'foo',
            'value': 15,
            'operator': '>'
        })
        actual_foo = jsonquery(self.session, self.model, json).one()
        expected_foo = self.query.filter(self.model.foo > 15).one()
        assert actual_foo is expected_foo

    def test_ge(self):
        json = jsonify({
            'column': 'foo',
            'value': 15,
            'operator': '>='
        })
        actual_foos = jsonquery(self.session, self.model, json).all()
        expected_foos = self.query.filter(self.model.foo >= 15).all()
        assert 2 == len(actual_foos) == len(expected_foos)
        assert set(actual_foos) == set(expected_foos)


class StringColumnTestCase(unittest.TestCase):

    def setUp(self):
        Base = declarative_base()

        class Foo(Base):
            __tablename__ = 'foos'
            id = Column(Integer, primary_key=True)
            foo = Column(String)
        engine = create_engine("sqlite://", echo=True)
        Base.metadata.create_all(engine)
        self.model = Foo
        self.session = sessionmaker(bind=engine)()

        self.add_foo(u'Hello')
        self.add_foo(u'hello')
        self.add_foo('HelloWorld')
        self.add_foo('helloworld')
        self.add_foo('HelloWorldString')
        self.add_foo('helloworldstring')

    def tearDown(self):
        self.session.close()

    def add_foo(self, value):
        foo = self.model(foo=value)
        self.session.add(foo)
        self.session.commit()

    @property
    def query(self):
        return self.session.query(self.model)

    def like_value(self, value):
        json = jsonify({
            'column': 'foo',
            'value': value,
            'operator': 'like'
        })
        actual_foos = jsonquery(self.session, self.model, json).all()
        expected_foos = self.query.filter(self.model.foo.like(value)).all()

        return actual_foos, expected_foos

    def test_basic_like_ignores_case(self):
        '''
        Test that like, ilike for a basic sqlite String column both ignore case

        Passing this test indicates that like, ilike are handled identically,
            and is the reason the various wildcard tests below do not have ilike
            versions.

        Should this test start failing, it may be because case sensitivity becomes the
            default for Column(String), in which case it would be relevant to have
            new like/ilike tests again.
        '''
        actual, expected = self.like_value('Hello')
        assert 2 == len(actual) == len(expected)
        assert set(actual) == set(expected)

    def test_prefix(self):
        actual, expected = self.like_value('Hello%')
        assert 6 == len(actual) == len(expected)
        assert set(actual) == set(expected)

    def test_suffix(self):
        actual, expected = self.like_value('%World')
        assert 2 == len(actual) == len(expected)
        assert set(actual) == set(expected)

    def test_prefix_and_suffix(self):
        actual, expected = self.like_value('%World%')
        assert 4 == len(actual) == len(expected)
        assert set(actual) == set(expected)
