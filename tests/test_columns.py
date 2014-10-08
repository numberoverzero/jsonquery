import json
import pytest
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from jsonquery import jsonquery


def jsonify(dict):
    # Easy validation that the test data isn't invalid json
    return json.loads(json.dumps(dict))


@pytest.fixture()
def user_setup(request):
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

    request.cls.model = User
    request.cls.engine = engine
    request.cls.session = sessionmaker(bind=engine)()


@pytest.fixture()
def foo_setup(request):
    Base = declarative_base()

    class Foo(Base):
        __tablename__ = 'foos'
        id = Column(Integer, primary_key=True)
        foo = Column(Integer)
    engine = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(engine)
    request.cls.model = Foo
    request.cls.engine = engine
    request.cls.session = sessionmaker(bind=engine)()


@pytest.fixture()
def string_setup(request):
    Base = declarative_base()

    class String(Base):
        __tablename__ = 'strings'
        id = Column(Integer, primary_key=True)
        string = Column(String)
    engine = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(engine)
    request.cls.model = String
    request.cls.engine = engine
    request.cls.session = sessionmaker(bind=engine)()


@pytest.mark.usefixtures("foo_setup")
class IntegerColumnTestCase():

    def setUp(self):
        self.add_foo(10)
        self.add_foo(15)
        self.add_foo(20)

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


@pytest.mark.usefixtures("string_setup")
class StringColumnTestCase():

    def setUp(self):
        self.add_string('Hello')
        self.add_string('hello')
        self.add_string('HelloWorld')
        self.add_string('helloworld')
        self.add_string('HelloWorldString')
        self.add_string('helloworldstring')

    def add_string(self, value):
        string = self.model(string=value)
        self.session.add(string)
        self.session.commit()

    @property
    def query(self):
        return self.session.query(self.model)

    def like_value(self, value):
        json = jsonify({
            'column': 'string',
            'value': value,
            'operator': 'like'
        })
        actual_strings = jsonquery(self.session, self.model, json).all()
        expected_strings = self.query.filter(
            self.model.string.like(value)).all()

        return actual_strings, expected_strings

    def test_basic_like_ignores_case(self):
        '''
        Test that like, ilike for a basic sqlite String column both ignore case

        Passing this test indicates that like, ilike are handled identically,
            and is the reason the various wildcard tests below do not have
            ilike versions.

        Should this test start failing, it may be because case sensitivity
            becomes the default for Column(String), in which case it would be
            relevant to have new like/ilike tests again.
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
