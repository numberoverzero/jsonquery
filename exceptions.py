class InterrogateException(Exception):
    pass


class InvalidQueryFormat(InterrogateException):
    pass


class IllegalConstraintException(InterrogateException):
    pass


class MaximumDepthException(InterrogateException):
    pass


class MaximumBreadthException(InterrogateException):
    pass


class MaximumElementsException(InterrogateException):
    pass
