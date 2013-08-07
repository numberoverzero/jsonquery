import ujson
import unittest
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import NullPool
from interrogate import Builder, exceptions


class InterrogateTestCase(unittest.TestCase):

    def valid_builder_args(self):
        model = self.model
        operators = {
            'and': ['AND'],
            'or': ['OR'],
            'not': ['NOT']
        }
        type_constraints = {
            'string': [
                'name',
                'email'
            ],
            'numeric': [
                'age',
                'height'
            ],
            'nullable': [
                'email',
                'height'
            ]
        }
        query_constraints = {
            'breadth': None,
            'depth': 32,
            'elements': 64
        }
        return [model, operators, type_constraints, query_constraints]

    def make_builder(self, model=None, operators=None, type_constraints=None, query_constraints=None):
        dm, do, dt, dq = self.valid_builder_args()

        return Builder(
            model or dm,
            operators or do,
            type_constraints or dt,
            query_constraints or dq
        )

    def setUp(self):
        Base = declarative_base()

        class User(Base):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            name = Column(String)
            email = Column(String)
            age = Column(Integer)
            height = Column(Integer)
        engine = create_engine("sqlite://", poolclass=NullPool)
        Base.metadata.create_all(engine)
        self.model = User
        self.session = sessionmaker(bind=engine)()

    def tearDown(self):
        self.session.close()

    def test_empty_model(self):
        args = self.valid_builder_args()
        args[0] = None
        with self.assertRaises(exceptions.IllegalConstraintException):
            return Builder(*args)

    def test_missing_operator(self):
        args = self.valid_builder_args()
        incomplete_operators = {
            'and': ['AND'],
            'or': ['OR'],
            # 'not': ['NOT']
        }
        args[1] = incomplete_operators
        with self.assertRaises(exceptions.IllegalConstraintException):
            return Builder(*args)

    def test_duplicate_type_constraint(self):
        args = self.valid_builder_args()
        dup_type_constraints = {
            'string': [
                'name',
                'email'
            ],
            'numeric': [
                'age',
                'height',
                'email'
            ],
            'nullable': [
                'email',
                'height'
            ]
        }
        args[2] = dup_type_constraints
        with self.assertRaises(exceptions.IllegalConstraintException):
            return Builder(*args)
