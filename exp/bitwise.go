package exp

type bitwise struct {
	lhs Expression
	rhs any
	op  BitwiseOperation
}

func NewBitwiseExpression(op BitwiseOperation, lhs Expression, rhs any) BitwiseExpression {
	return bitwise{op: op, lhs: lhs, rhs: rhs}
}

func (b bitwise) Clone() Expression {
	return NewBitwiseExpression(b.op, b.lhs.Clone(), b.rhs)
}

func (b bitwise) RHS() any {
	return b.rhs
}

func (b bitwise) LHS() Expression {
	return b.lhs
}

func (b bitwise) Op() BitwiseOperation {
	return b.op
}

func (b bitwise) Expression() Expression                   { return b }
func (b bitwise) As(val any) AliasedExpression             { return NewAliasExpression(b, val) }
func (b bitwise) Eq(val any) BooleanExpression             { return eq(b, val) }
func (b bitwise) Neq(val any) BooleanExpression            { return neq(b, val) }
func (b bitwise) Gt(val any) BooleanExpression             { return gt(b, val) }
func (b bitwise) Gte(val any) BooleanExpression            { return gte(b, val) }
func (b bitwise) Lt(val any) BooleanExpression             { return lt(b, val) }
func (b bitwise) Lte(val any) BooleanExpression            { return lte(b, val) }
func (b bitwise) Asc() OrderedExpression                   { return asc(b) }
func (b bitwise) Desc() OrderedExpression                  { return desc(b) }
func (b bitwise) Like(i any) BooleanExpression             { return like(b, i) }
func (b bitwise) NotLike(i any) BooleanExpression          { return notLike(b, i) }
func (b bitwise) ILike(i any) BooleanExpression            { return iLike(b, i) }
func (b bitwise) NotILike(i any) BooleanExpression         { return notILike(b, i) }
func (b bitwise) RegexpLike(val any) BooleanExpression     { return regexpLike(b, val) }
func (b bitwise) RegexpNotLike(val any) BooleanExpression  { return regexpNotLike(b, val) }
func (b bitwise) RegexpILike(val any) BooleanExpression    { return regexpILike(b, val) }
func (b bitwise) RegexpNotILike(val any) BooleanExpression { return regexpNotILike(b, val) }
func (b bitwise) In(i ...any) BooleanExpression            { return in(b, i...) }
func (b bitwise) NotIn(i ...any) BooleanExpression         { return notIn(b, i...) }
func (b bitwise) Is(i any) BooleanExpression               { return is(b, i) }
func (b bitwise) IsNot(i any) BooleanExpression            { return isNot(b, i) }
func (b bitwise) IsNull() BooleanExpression                { return is(b, nil) }
func (b bitwise) IsNotNull() BooleanExpression             { return isNot(b, nil) }
func (b bitwise) IsTrue() BooleanExpression                { return is(b, true) }
func (b bitwise) IsNotTrue() BooleanExpression             { return isNot(b, true) }
func (b bitwise) IsFalse() BooleanExpression               { return is(b, false) }
func (b bitwise) IsNotFalse() BooleanExpression            { return isNot(b, false) }
func (b bitwise) Distinct() SQLFunctionExpression          { return NewSQLFunctionExpression("DISTINCT", b) }
func (b bitwise) Between(val RangeVal) RangeExpression     { return between(b, val) }
func (b bitwise) NotBetween(val RangeVal) RangeExpression  { return notBetween(b, val) }

// used internally to create a Bitwise Inversion BitwiseExpression
func bitwiseInversion(rhs Expression) BitwiseExpression {
	return NewBitwiseExpression(BitwiseInversionOp, nil, rhs)
}

// used internally to create a Bitwise OR BitwiseExpression
func bitwiseOr(lhs Expression, rhs any) BitwiseExpression {
	return NewBitwiseExpression(BitwiseOrOp, lhs, rhs)
}

// used internally to create a Bitwise AND BitwiseExpression
func bitwiseAnd(lhs Expression, rhs any) BitwiseExpression {
	return NewBitwiseExpression(BitwiseAndOp, lhs, rhs)
}

// used internally to create a Bitwise XOR BitwiseExpression
func bitwiseXor(lhs Expression, rhs any) BitwiseExpression {
	return NewBitwiseExpression(BitwiseXorOp, lhs, rhs)
}

// used internally to create a Bitwise LEFT SHIFT BitwiseExpression
func bitwiseLeftShift(lhs Expression, rhs any) BitwiseExpression {
	return NewBitwiseExpression(BitwiseLeftShiftOp, lhs, rhs)
}

// used internally to create a Bitwise RIGHT SHIFT BitwiseExpression
func bitwiseRightShift(lhs Expression, rhs any) BitwiseExpression {
	return NewBitwiseExpression(BitwiseRightShiftOp, lhs, rhs)
}
