import ujson
import unittest
from sqlalchemy import Column, Integer, String, create_engine, and_, or_, not_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from interrogate import jsonquery


def jsonify(dict):
    # Easy validation that the test data isn't invalid json
    return ujson.loads(ujson.dumps(dict))


class InterrogateTestCase(unittest.TestCase):

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
