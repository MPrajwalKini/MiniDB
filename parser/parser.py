"""
MiniDB SQL Parser
=================
Recursive-descent parser for SQL.
Converts a stream of tokens into an AST.

Architecture:
- Input: Immutable list of Tokens (from Tokenizer)
- Output: Statement AST node
- Lookahead: 1 token (LL(1) mostly, sometimes 2 for specific constructs)
"""

from typing import List, Optional, NoReturn

from parser.tokenizer import Token, TokenType, Tokenizer
from parser.ast_nodes import (
    Statement, SelectStmt, InsertStmt, UpdateStmt, DeleteStmt, CreateTableStmt,
    CreateIndexStmt, DropIndexStmt,
    BeginStmt, CommitStmt, RollbackStmt, ExplainStmt,
    Expression, Literal, QualifiedName, BinaryExpr, UnaryExpr, GroupingExpr, IsNullExpr,
    SelectItem, OrderItem, ColumnDef, Assignment
)
from storage.types import DataType


class ParseError(Exception):
    """Error during parsing with position info."""
    def __init__(self, message: str, token: Token):
        super().__init__(f"{message} at line {token.line}:{token.col}")
        self.token = token


class Parser:
    """
    Recursive-descent SQL parser.
    Initialize with a list of tokens, call .parse() to get the AST.
    """

    def __init__(self, tokens: List[Token]):
        self._tokens = tokens
        self._pos = 0
        self._len = len(tokens)

    def parse(self) -> Statement:
        """Parse a single SQL statement."""
        if self._match(TokenType.EOF):
            # Empty string or just comments
            raise ParseError("Unexpected end of input", self._peek())
        
        stmt = self._parse_statement()
        
        # Ensure we consumed everything (except maybe EOF)
        if not self._is_at_end() and self._peek().type != TokenType.EOF:
             # If we have a semicolon, consume it and check for EOF
            if self._match(TokenType.SEMICOLON):
                if not self._is_at_end() and self._peek().type != TokenType.EOF:
                     raise ParseError("Multiple statements not supported", self._peek())
            else:
                raise ParseError("Unexpected token after statement", self._peek())
                
        return stmt

    # ─── Statement Parsing ──────────────────────────────────────────

    def _parse_statement(self) -> Statement:
        if self._match(TokenType.SELECT):
            return self._parse_select()
        if self._match(TokenType.INSERT):
            return self._parse_insert()
        if self._match(TokenType.UPDATE):
            return self._parse_update()
        if self._match(TokenType.DELETE):
            return self._parse_delete()
        if self._match(TokenType.DELETE):
            return self._parse_delete()
        if self._match(TokenType.CREATE):
            return self._parse_create()
        if self._match(TokenType.DROP):
            return self._parse_drop()
        if self._match(TokenType.BEGIN):
            return self._parse_begin()
        if self._match(TokenType.COMMIT):
            return CommitStmt()
        if self._match(TokenType.ROLLBACK):
            return RollbackStmt()
        if self._match(TokenType.EXPLAIN):
            return self._parse_explain()
        
        raise ParseError(f"Unexpected token {self._peek().value}, expected statement", self._peek())

    def _parse_begin(self) -> BeginStmt:
        """Parse BEGIN [TRANSACTION]."""
        self._match(TokenType.TRANSACTION)  # Optional TRANSACTION keyword
        return BeginStmt()

    def _parse_explain(self) -> ExplainStmt:
        """Parse EXPLAIN [LOGICAL|PHYSICAL] <statement>."""
        level = "both"
        if self._match(TokenType.LOGICAL):
            level = "logical"
        elif self._match(TokenType.PHYSICAL):
            level = "physical"
        inner = self._parse_statement()
        return ExplainStmt(inner=inner, level=level)

    def _parse_select(self) -> SelectStmt:
        # SELECT [DISTINCT] ...
        distinct = self._match(TokenType.DISTINCT)
        
        # Parse select list
        columns = self._parse_select_list()
        
        # FROM clause (optional)
        from_table = None
        if self._match(TokenType.FROM):
             from_table = self._parse_qualified_name()
             
        # WHERE clause
        where = None
        if self._match(TokenType.WHERE):
            where = self._parse_expression()
            
        # ORDER BY
        order_by = None
        if self._match(TokenType.ORDER):
            self._consume(TokenType.BY, "Expected BY after ORDER")
            order_by = self._parse_order_list()
            
        # LIMIT
        limit = None
        if self._match(TokenType.LIMIT):
            limit = self._parse_expression()
            
        return SelectStmt(
            columns=columns,
            from_table=from_table,
            where=where,
            order_by=order_by,
            limit=limit,
            distinct=distinct
        )

    def _parse_insert(self) -> InsertStmt:
        # INSERT INTO table (cols...) VALUES (vals...)
        self._consume(TokenType.INTO, "Expected INTO after INSERT")
        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
        
        columns = None
        if self._match(TokenType.LPAREN):
            columns = []
            while True:
                col_name = self._consume(TokenType.IDENTIFIER, "Expected column name").value
                columns.append(col_name)
                if not self._match(TokenType.COMMA):
                    break
            self._consume(TokenType.RPAREN, "Expected ) after column list")
            
        self._consume(TokenType.VALUES, "Expected VALUES")
        self._consume(TokenType.LPAREN, "Expected ( before values")
        
        values = []
        while True:
            values.append(self._parse_expression())
            if not self._match(TokenType.COMMA):
                break
        
        self._consume(TokenType.RPAREN, "Expected ) after values")
        
        return InsertStmt(table_name, columns, values)

    def _parse_update(self) -> UpdateStmt:
        # UPDATE table SET col=val, ... WHERE ...
        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
        self._consume(TokenType.SET, "Expected SET after table name")
        
        assignments = []
        while True:
            col = self._consume(TokenType.IDENTIFIER, "Expected column name").value
            self._consume(TokenType.EQ, "Expected = in assignment")
            val = self._parse_expression()
            assignments.append(Assignment(col, val))
            if not self._match(TokenType.COMMA):
                break
                
        where = None
        if self._match(TokenType.WHERE):
            where = self._parse_expression()
            
        return UpdateStmt(table_name, assignments, where)

    def _parse_delete(self) -> DeleteStmt:
        # DELETE FROM table WHERE ...
        self._consume(TokenType.FROM, "Expected FROM after DELETE")
        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
        
        where = None
        if self._match(TokenType.WHERE):
            where = self._parse_expression()
            
        return DeleteStmt(table_name, where)

    def _parse_create(self) -> Statement:
        # CREATE TABLE ... or CREATE INDEX ...
        if self._match(TokenType.TABLE):
            return self._parse_create_table()
        if self._match(TokenType.INDEX):
            return self._parse_create_index()
        
        raise ParseError(f"Expected TABLE or INDEX after CREATE", self._peek())

    def _parse_create_table(self) -> CreateTableStmt:
        # CREATE TABLE table (col type ...)
        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
        
        self._consume(TokenType.LPAREN, "Expected ( after table name")
        
        columns = []
        while True:
            # Column definition: name type [NOT NULL]
            name = self._consume(TokenType.IDENTIFIER, "Expected column name").value
            
            # Parse Data Type
            type_token = self._advance()
            data_type = self._map_data_type(type_token)
            
            # Helper for VARCHAR(N)
            if data_type == DataType.STRING and self._check(TokenType.LPAREN):
                 # Skip the length part safely if present (e.g. VARCHAR(255))
                 # For now we treat VARCHAR same as STRING
                 self._advance() # (
                 self._consume(TokenType.NUMBER, "Expected length for VARCHAR")
                 self._consume(TokenType.RPAREN, "Expected )")

            # Check for NOT NULL
            nullable = True
            if self._match(TokenType.NOT):
                self._consume(TokenType.NULL, "Expected NULL after NOT")
                nullable = False
            
            columns.append(ColumnDef(name, data_type, nullable))
            
            if not self._match(TokenType.COMMA):
                break
                
        self._consume(TokenType.RPAREN, "Expected ) after column definitions")
        return CreateTableStmt(table_name, columns)

    def _parse_create_index(self) -> CreateIndexStmt:
        # CREATE INDEX index_name ON table_name (column_name)
        index_name = self._consume(TokenType.IDENTIFIER, "Expected index name").value
        self._consume(TokenType.ON, "Expected ON after index name")
        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
        
        self._consume(TokenType.LPAREN, "Expected (")
        column_name = self._consume(TokenType.IDENTIFIER, "Expected column name").value
        self._consume(TokenType.RPAREN, "Expected )")
        
        return CreateIndexStmt(index_name, table_name, column_name)

    def _parse_drop(self) -> Statement:
        # DROP INDEX index_name
        # (DROP TABLE not yet supported in AST)
        if self._match(TokenType.INDEX):
             index_name = self._consume(TokenType.IDENTIFIER, "Expected index name").value
             return DropIndexStmt(index_name)
             
        if self._match(TokenType.TABLE):
            raise ParseError("DROP TABLE not yet implemented", self._previous())

        raise ParseError("Expected INDEX after DROP", self._peek())

    # ─── Expression Parsing ─────────────────────────────────────────
    # Precedence climbing: OR -> AND -> NOT -> Comparison -> Add -> Mult -> Unary -> Primary

    def _parse_expression(self) -> Expression:
        return self._parse_or()

    def _parse_or(self) -> Expression:
        expr = self._parse_and()
        while self._match(TokenType.OR):
            op = self._previous().type
            right = self._parse_and()
            expr = BinaryExpr(expr, op, right)
        return expr

    def _parse_and(self) -> Expression:
        expr = self._parse_not()
        while self._match(TokenType.AND):
            op = self._previous().type
            right = self._parse_not()
            expr = BinaryExpr(expr, op, right)
        return expr

    def _parse_not(self) -> Expression:
        if self._match(TokenType.NOT):
            op = self._previous().type
            operand = self._parse_not()
            return UnaryExpr(op, operand)
        return self._parse_comparison()

    def _parse_comparison(self) -> Expression:
        expr = self._parse_addition()
        
        # Handle IS NULL / IS NOT NULL
        if self._match(TokenType.IS):
            not_null = self._match(TokenType.NOT)
            self._consume(TokenType.NULL, "Expected NULL after IS")
            return IsNullExpr(expr, not_null)
            
        # Handle normal comparison ops
        if self._match(TokenType.EQ, TokenType.NEQ, TokenType.LT, 
                       TokenType.GT, TokenType.LTE, TokenType.GTE):
            op = self._previous().type
            right = self._parse_addition()
            return BinaryExpr(expr, op, right)
            
        return expr

    def _parse_addition(self) -> Expression:
        expr = self._parse_multiplication()
        while self._match(TokenType.PLUS, TokenType.MINUS):
            op = self._previous().type
            right = self._parse_multiplication()
            expr = BinaryExpr(expr, op, right)
        return expr

    def _parse_multiplication(self) -> Expression:
        expr = self._parse_unary()
        while self._match(TokenType.STAR, TokenType.SLASH):
            op = self._previous().type
            right = self._parse_unary()
            expr = BinaryExpr(expr, op, right)
        return expr

    def _parse_unary(self) -> Expression:
        if self._match(TokenType.MINUS, TokenType.PLUS):
            op = self._previous().type
            operand = self._parse_unary()
            return UnaryExpr(op, operand)
        return self._parse_primary()

    def _parse_primary(self) -> Expression:
        if self._match(TokenType.FALSE): return Literal(False, DataType.BOOLEAN)
        if self._match(TokenType.TRUE): return Literal(True, DataType.BOOLEAN)
        if self._match(TokenType.NULL): return Literal(None, DataType.INT) # Type inferred later? No, strictly typed... used int as placeholder or maybe new NULL type? Using inner value None for now.
        
        if self._match(TokenType.NUMBER):
            val_str = self._previous().value
            if '.' in val_str:
                return Literal(float(val_str), DataType.FLOAT)
            return Literal(int(val_str), DataType.INT)
            
        if self._match(TokenType.STRING_LIT):
            return Literal(self._previous().value, DataType.STRING)
            
        if self._match(TokenType.LPAREN):
            expr = self._parse_expression()
            self._consume(TokenType.RPAREN, "Expected ) after expression")
            return GroupingExpr(expr)
            
        # Qualified Name (identifier . identifier)
        if self._check(TokenType.IDENTIFIER):
             return self._parse_qualified_name()
             
        if self._check(TokenType.EOF):
            raise ParseError("Unexpected end of input", self._peek())
             
        raise ParseError(f"Unexpected token {self._peek().value}, expected expression", self._peek())

    # ─── Helpers ────────────────────────────────────────────────────

    def _parse_qualified_name(self) -> QualifiedName:
        parts = []
        parts.append(self._consume(TokenType.IDENTIFIER, "Expected identifier").value)
        while self._match(TokenType.DOT):
             parts.append(self._consume(TokenType.IDENTIFIER, "Expected identifier after dot").value)
        return QualifiedName(parts)

    def _parse_select_list(self) -> List[SelectItem]:
        items = []
        if self._match(TokenType.STAR):
             # Represent * as a special QualifiedName(["*"])
             items.append(SelectItem(QualifiedName(["*"])))
             return items

        # Otherwise parse list of expressions
        while True:
            expr = self._parse_expression()
            alias = None
            if self._match(TokenType.AS):
                alias = self._consume(TokenType.IDENTIFIER, "Expected alias").value
            elif self._check(TokenType.IDENTIFIER) and not self._is_at_end():
                # Optional AS. Careful with keywords.
                # If next is FROM, WHERE, etc. we stop.
                # But FROM is handled by caller.
                # If we are here, next token is IDENTIFIER.
                # Is it a keyword acting as Alias? Or next clause?
                # Parser consumes generic IDENTIFIER.
                # Keywords are tokenized as KEYWORDS, not generic IDENTIFIERS unless quoted.
                # So if next token.type is IDENTIFIER, it IS an alias.
                alias = self._advance().value
                
            items.append(SelectItem(expr, alias))
            if not self._match(TokenType.COMMA):
                break
        return items

    def _parse_order_list(self) -> List[OrderItem]:
        items = []
        while True:
            expr = self._parse_expression()
            ascending = True
            if self._match(TokenType.DESC):
                ascending = False
            elif self._match(TokenType.ASC):
                ascending = True
            items.append(OrderItem(expr, ascending))
            if not self._match(TokenType.COMMA):
                break
        return items

    def _map_data_type(self, token: Token) -> DataType:
        if token.type == TokenType.INT: return DataType.INT
        if token.type == TokenType.FLOAT: return DataType.FLOAT
        if token.type == TokenType.STRING: return DataType.STRING
        if token.type == TokenType.BOOLEAN: return DataType.BOOLEAN
        if token.type == TokenType.DATE: return DataType.DATE
        if token.type == TokenType.VARCHAR: return DataType.STRING
        raise ParseError(f"Unknown data type {token.value}", token)

    # ─── Core Parser Logic ──────────────────────────────────────────

    def _peek(self) -> Token:
        if self._pos >= len(self._tokens):
            return self._tokens[-1] # EOF
        return self._tokens[self._pos]

    def _previous(self) -> Token:
        return self._tokens[self._pos - 1]

    def _is_at_end(self) -> bool:
        return self._peek().type == TokenType.EOF

    def _check(self, type: TokenType) -> bool:
        if self._is_at_end() and type != TokenType.EOF:
            return False
        return self._peek().type == type

    def _advance(self) -> Token:
        if not self._is_at_end():
            self._pos += 1
        return self._previous()

    def _match(self, *types: TokenType) -> bool:
        for type in types:
            if self._check(type):
                self._advance()
                return True
        return False

    def _consume(self, type: TokenType, message: str) -> Token:
        if self._check(type):
            return self._advance()
        raise ParseError(message, self._peek())
