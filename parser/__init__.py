"""
MiniDB SQL Parser
=================
Public API for the SQL parser.

Usage:
    from parser import parse, ParseError

    ast = parse("SELECT * FROM users")
    print(ast)
"""

from parser.parser import Parser, ParseError
from parser.tokenizer import Tokenizer, Token, TokenType
from parser.ast_nodes import Statement

def parse(sql: str) -> Statement:
    """
    Parse a SQL string into an AST Statement.
    Raises ParseError if syntax is invalid.
    """
    tokenizer = Tokenizer()
    tokens = tokenizer.tokenize(sql)
    parser = Parser(tokens)
    return parser.parse()

def tokenize(sql: str) -> list[Token]:
    """Tokenize SQL string (for debugging)."""
    return Tokenizer().tokenize(sql)
