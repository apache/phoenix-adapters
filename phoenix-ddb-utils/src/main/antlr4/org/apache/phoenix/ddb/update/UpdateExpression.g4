grammar UpdateExpression;

updateExpression
    : clause+ EOF
    ;

clause
    : SET    setAction    (',' setAction)*       # setClause
    | REMOVE removeAction (',' removeAction)*    # removeClause
    | ADD    addAction    (',' addAction)*       # addClause
    | DELETE deleteAction (',' deleteAction)*    # deleteClause
    ;

setAction
    : path '=' setValue
    ;

setValue
    : operand                       # setValueOperand
    | operand '+' operand           # setValueAdd
    | operand '-' operand           # setValueSubtract
    ;

operand
    : path                          # operandPath
    | placeholder                   # operandPlaceholder
    | ifNotExists                   # operandIfNotExists
    | listAppend                    # operandListAppend
    ;

ifNotExists
    : IF_NOT_EXISTS '(' path ',' ifNotExistsValue ')'
    ;

ifNotExistsValue
    : path                          # ifNotExistsValuePath
    | placeholder                   # ifNotExistsValuePlaceholder
    ;

listAppend
    : LIST_APPEND '(' listAppendOperand ',' listAppendOperand ')'
    ;

listAppendOperand
    : path                          # listAppendOperandPath
    | placeholder                   # listAppendOperandPlaceholder
    | ifNotExists                   # listAppendOperandIfNotExists
    ;

removeAction
    : path
    ;

addAction
    : path placeholder
    ;

deleteAction
    : path placeholder
    ;

path
    : pathStep pathSuffix*
    ;

pathSuffix
    : '.' pathStep
    | '[' INT ']'
    ;

pathStep
    : NAME
    | ALIAS
    ;

placeholder
    : VALUE_PLACEHOLDER
    ;

// Lexer rules. Order matters: keywords before NAME so they win over the generic NAME rule.
// Clause keywords are case-insensitive to match DDB (e.g. `set a = :v` is valid).
// Function names are case-sensitive per AWS docs ("The function name is case sensitive").
SET            : S E T ;
REMOVE         : R E M O V E ;
ADD            : A D D ;
DELETE         : D E L E T E ;
IF_NOT_EXISTS  : 'if_not_exists' ;
LIST_APPEND    : 'list_append' ;

ALIAS              : '#' [a-zA-Z0-9_]+ ;
VALUE_PLACEHOLDER  : ':' [a-zA-Z0-9_]+ ;
NAME               : [a-zA-Z] [a-zA-Z0-9_]* ;
INT                : [0-9]+ ;

WS : [ \t\r\n]+ -> skip ;

fragment A : [aA] ;
fragment D : [dD] ;
fragment E : [eE] ;
fragment L : [lL] ;
fragment M : [mM] ;
fragment O : [oO] ;
fragment R : [rR] ;
fragment S : [sS] ;
fragment T : [tT] ;
fragment V : [vV] ;
