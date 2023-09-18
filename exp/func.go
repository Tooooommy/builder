package exp

type (
	sqlFunctionExpression struct {
		name string
		args []any
	}
)

// Creates a new SQLFunctionExpression with the given name and arguments
func NewSQLFunctionExpression(name string, args ...any) SQLFunctionExpression {
	return sqlFunctionExpression{name: name, args: args}
}

func (sfe sqlFunctionExpression) Clone() Expression {
	return sqlFunctionExpression{name: sfe.name, args: sfe.args}
}

func (sfe sqlFunctionExpression) Expression() Expression { return sfe }

func (sfe sqlFunctionExpression) Args() []any { return sfe.args }

func (sfe sqlFunctionExpression) Name() string { return sfe.name }

func (sfe sqlFunctionExpression) As(val any) AliasedExpression {
	return NewAliasExpression(sfe, val)
}

func (sfe sqlFunctionExpression) Eq(val any) BooleanExpression  { return eq(sfe, val) }
func (sfe sqlFunctionExpression) Neq(val any) BooleanExpression { return neq(sfe, val) }

func (sfe sqlFunctionExpression) Gt(val any) BooleanExpression  { return gt(sfe, val) }
func (sfe sqlFunctionExpression) Gte(val any) BooleanExpression { return gte(sfe, val) }
func (sfe sqlFunctionExpression) Lt(val any) BooleanExpression  { return lt(sfe, val) }
func (sfe sqlFunctionExpression) Lte(val any) BooleanExpression { return lte(sfe, val) }

func (sfe sqlFunctionExpression) Between(val RangeVal) RangeExpression { return between(sfe, val) }

func (sfe sqlFunctionExpression) NotBetween(val RangeVal) RangeExpression {
	return notBetween(sfe, val)
}

func (sfe sqlFunctionExpression) Like(val any) BooleanExpression    { return like(sfe, val) }
func (sfe sqlFunctionExpression) NotLike(val any) BooleanExpression { return notLike(sfe, val) }
func (sfe sqlFunctionExpression) ILike(val any) BooleanExpression   { return iLike(sfe, val) }

func (sfe sqlFunctionExpression) NotILike(val any) BooleanExpression {
	return notILike(sfe, val)
}

func (sfe sqlFunctionExpression) RegexpLike(val any) BooleanExpression {
	return regexpLike(sfe, val)
}

func (sfe sqlFunctionExpression) RegexpNotLike(val any) BooleanExpression {
	return regexpNotLike(sfe, val)
}

func (sfe sqlFunctionExpression) RegexpILike(val any) BooleanExpression {
	return regexpILike(sfe, val)
}

func (sfe sqlFunctionExpression) RegexpNotILike(val any) BooleanExpression {
	return regexpNotILike(sfe, val)
}

func (sfe sqlFunctionExpression) In(vals ...any) BooleanExpression { return in(sfe, vals...) }
func (sfe sqlFunctionExpression) NotIn(vals ...any) BooleanExpression {
	return notIn(sfe, vals...)
}
func (sfe sqlFunctionExpression) Is(val any) BooleanExpression    { return is(sfe, val) }
func (sfe sqlFunctionExpression) IsNot(val any) BooleanExpression { return isNot(sfe, val) }
func (sfe sqlFunctionExpression) IsNull() BooleanExpression       { return is(sfe, nil) }
func (sfe sqlFunctionExpression) IsNotNull() BooleanExpression    { return isNot(sfe, nil) }
func (sfe sqlFunctionExpression) IsTrue() BooleanExpression       { return is(sfe, true) }
func (sfe sqlFunctionExpression) IsNotTrue() BooleanExpression    { return isNot(sfe, true) }
func (sfe sqlFunctionExpression) IsFalse() BooleanExpression      { return is(sfe, false) }
func (sfe sqlFunctionExpression) IsNotFalse() BooleanExpression   { return isNot(sfe, false) }

func (sfe sqlFunctionExpression) Over(we WindowExpression) SQLWindowFunctionExpression {
	return NewSQLWindowFunctionExpression(sfe, nil, we)
}

func (sfe sqlFunctionExpression) OverName(windowName IdentifierExpression) SQLWindowFunctionExpression {
	return NewSQLWindowFunctionExpression(sfe, windowName, nil)
}

func (sfe sqlFunctionExpression) Asc() OrderedExpression  { return asc(sfe) }
func (sfe sqlFunctionExpression) Desc() OrderedExpression { return desc(sfe) }
