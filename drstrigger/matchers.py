class Matcher(object):
    @classmethod
    def parse(cls, pattern):
        if pattern == '*':
            return Any()
        if pattern.startswith('>='):
            return GreaterOrEq(int(pattern[len('>='):].strip()))
        if pattern.startswith('>'):
            return GreaterThan(int(pattern[len('>'):].strip()))
        if pattern.startswith('<='):
            return LessOrEq(int(pattern[len('<='):].strip()))
        if pattern.startswith('<'):
            return LessThan(int(pattern[len('<'):].strip()))
        if pattern.startswith('='):
            return Equals(int(pattern[len('='):].strip()))
        if pattern.count('-') == 1:
            return Between([int(temp) for temp in pattern.split('-')])
        if '|' in pattern:
            return OneOf([temp.strip() for temp in pattern.split('|')])
        return Equals(pattern)


class BaseMatcher(object):
    # turn into AbstractBaseClass? TODO
    def __init__(self, value):
        self.value = value


class Equals(BaseMatcher):
    def match(self, value):
        return value == self.value


class OneOf(BaseMatcher):
    def match(self, value):
        return value in self.value


class GreaterOrEq(BaseMatcher):
    def match(self, value):
        return value >= self.value


class GreaterThan(BaseMatcher):
    def match(self, value):
        return value > self.value


class LessOrEq(BaseMatcher):
    def match(self, value):
        return value <= self.value


class LessThan(BaseMatcher):
    def match(self, value):
        return value < self.value


class Between(BaseMatcher):
    def match(self, value):
        return self.value[0] <= value and value <= self.value[1]


class Any(BaseMatcher):
    def __init__(self):
        super(Any, self).__init__(None)

    def match(self, value):
        return True
