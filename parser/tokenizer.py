"""
MiniDB SQL Tokenizer
====================
Converts raw SQL strings into a stream of typed tokens.

Features:
- Case-insensitive keywords (SELECT = select)
- Quoted identifiers ("My Table")
- String literals ('hello world')
- Numeric literals (integers and floats)
- Operators and punctuation
- Line/column tracking for error reporting
- EOF sentinel token
"""

import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List


class TokenType(Enum):
    # Keywords
    SELECT = auto()
    FROM = auto()
    WHERE = auto()
    INSERT = auto()
    INTO = auto()
    VALUES = auto()
    DELETE = auto()
    UPDATE = auto()
    SET = auto()
    CREATE = auto()
    TABLE = auto()
    DROP = auto()
    AND = auto()
    OR = auto()
    NOT = auto()
    NULL = auto()
    TRUE = auto()
    FALSE = auto()
    ORDER = auto()
    BY = auto()
    ASC = auto()
    DESC = auto()
    LIMIT = auto()
    AS = auto()
    DISTINCT = auto()  # Placeholder
    IS = auto()        # Placeholder

    # Transaction Control
    BEGIN = auto()
    COMMIT = auto()
    ROLLBACK = auto()
    TRANSACTION = auto()

    # Query Analysis
    EXPLAIN = auto()
    LOGICAL = auto()
    PHYSICAL = auto()

    # Data Types
    INT = auto()
    FLOAT = auto()
    STRING = auto()
    BOOLEAN = auto()
    DATE = auto()
    VARCHAR = auto()

    # Literals
    NUMBER = auto()      # 123, 3.14
    STRING_LIT = auto()  # 'hello'
    IDENTIFIER = auto()  # table_name, "Quoted Name"

    # Operators
    EQ = auto()          # =
    NEQ = auto()         # != or <>
    LT = auto()          # <
    GT = auto()          # >
    LTE = auto()         # <=
    GTE = auto()         # >=
    PLUS = auto()        # +
    MINUS = auto()       # -
    STAR = auto()        # *
    SLASH = auto()       # /

    # Punctuation
    LPAREN = auto()      # (
    RPAREN = auto()      # )
    COMMA = auto()       # ,
    DOT = auto()         # .
    SEMICOLON = auto()   # ;

    # Special
    EOF = auto()


@dataclass(frozen=True)
class Token:
    """Immutable token with position info."""
    type: TokenType
    value: str
    line: int
    col: int

    def __repr__(self) -> str:
        return f"Token({self.type.name}, '{self.value}', {self.line}:{self.col})"


class Tokenizer:
    """
    Lexer for SQL. call .tokenize(sql) to get a list of tokens.
    """

    # Keyword map (uppercase for normalization)
    KEYWORDS = {
        "SELECT": TokenType.SELECT,
        "FROM": TokenType.FROM,
        "WHERE": TokenType.WHERE,
        "INSERT": TokenType.INSERT,
        "INTO": TokenType.INTO,
        "VALUES": TokenType.VALUES,
        "DELETE": TokenType.DELETE,
        "UPDATE": TokenType.UPDATE,
        "SET": TokenType.SET,
        "CREATE": TokenType.CREATE,
        "TABLE": TokenType.TABLE,
        "DROP": TokenType.DROP,
        "AND": TokenType.AND,
        "OR": TokenType.OR,
        "NOT": TokenType.NOT,
        "NULL": TokenType.NULL,
        "TRUE": TokenType.TRUE,
        "FALSE": TokenType.FALSE,
        "ORDER": TokenType.ORDER,
        "BY": TokenType.BY,
        "ASC": TokenType.ASC,
        "DESC": TokenType.DESC,
        "LIMIT": TokenType.LIMIT,
        "AS": TokenType.AS,
        "DISTINCT": TokenType.DISTINCT,
        "IS": TokenType.IS,
        "INT": TokenType.INT,
        "INTEGER": TokenType.INT,
        "FLOAT": TokenType.FLOAT,
        "STRING": TokenType.STRING,
        "TEXT": TokenType.STRING,
        "BOOLEAN": TokenType.BOOLEAN,
        "BOOL": TokenType.BOOLEAN,
        "DATE": TokenType.DATE,
        "VARCHAR": TokenType.VARCHAR,
        "BEGIN": TokenType.BEGIN,
        "COMMIT": TokenType.COMMIT,
        "ROLLBACK": TokenType.ROLLBACK,
        "TRANSACTION": TokenType.TRANSACTION,
        "EXPLAIN": TokenType.EXPLAIN,
        "LOGICAL": TokenType.LOGICAL,
        "PHYSICAL": TokenType.PHYSICAL,
    }

    # Regex patterns
    # Note: order matters!
    PATTERNS = [
        # Whitespace (skip)
        (re.compile(r'\s+'), None),
        # Comments (skip) -- and /* */
        (re.compile(r'--.*'), None),
        (re.compile(r'/\*.*?\*/', re.DOTALL), None),
        
        # Operators (multi-char first)
        (re.compile(r'>='), TokenType.GTE),
        (re.compile(r'<='), TokenType.LTE),
        (re.compile(r'!='), TokenType.NEQ),
        (re.compile(r'<>'), TokenType.NEQ),
        (re.compile(r'='), TokenType.EQ),
        (re.compile(r'<'), TokenType.LT),
        (re.compile(r'>'), TokenType.GT),
        (re.compile(r'\+'), TokenType.PLUS),
        (re.compile(r'-'), TokenType.MINUS),
        (re.compile(r'\*'), TokenType.STAR),
        (re.compile(r'/'), TokenType.SLASH),

        # Punctuation
        (re.compile(r'\('), TokenType.LPAREN),
        (re.compile(r'\)'), TokenType.RPAREN),
        (re.compile(r','), TokenType.COMMA),
        (re.compile(r'\.'), TokenType.DOT),
        (re.compile(r';'), TokenType.SEMICOLON),

        # Literals
        # String: 'hello' (supports escaped single quote via '')
        (re.compile(r"'((?:''|[^'])*)'"), TokenType.STRING_LIT),
        # Number: 123.45 or 123
        (re.compile(r'\d+\.\d+'), TokenType.NUMBER),
        (re.compile(r'\d+'), TokenType.NUMBER),

        # Identifiers / Keywords
        # Quoted identifier: "My Table"
        (re.compile(r'"([^"]+)"'), TokenType.IDENTIFIER),
        # Unquoted word: my_table (could be keyword)
        (re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*'), TokenType.IDENTIFIER),
    ]

    def tokenize(self, sql: str) -> List[Token]:
        """Tokenize SQL string into a list of Tokens."""
        tokens = []
        pos = 0
        line = 1
        col_start = 0  # position of start of current line in string
        
        while pos < len(sql):
            match = None
            
            # Try all regex patterns
            for pattern, token_type in self.PATTERNS:
                regex_match = pattern.match(sql, pos)
                if regex_match:
                    text = regex_match.group(0)
                    
                    if token_type:  # If not skipped (whitespace/comments)
                        # Handle keywords
                        if token_type == TokenType.IDENTIFIER and not text.startswith('"'):
                            upper_text = text.upper()
                            if upper_text in self.KEYWORDS:
                                token_type = self.KEYWORDS[upper_text]
                        
                        # Handle captured groups for string/quoted identifiers
                        value = text
                        if token_type == TokenType.STRING_LIT:
                            # Unescape '' -> '
                            value = regex_match.group(1).replace("''", "'")
                        elif token_type == TokenType.IDENTIFIER and text.startswith('"'):
                            value = regex_match.group(1)
                            
                        # Calculate column
                        col = pos - col_start + 1
                        tokens.append(Token(token_type, value, line, col))
                    
                    # Advance position
                    pos += len(text)
                    
                    # Update line/col tracking
                    newlines = text.count('\n')
                    if newlines > 0:
                        line += newlines
                        # New column start is after the last newline
                        col_start = pos - (len(text) - text.rfind('\n') - 1)
                    
                    match = regex_match
                    break
            
            if not match:
                # Error: unexpected character
                col = pos - col_start + 1
                char = sql[pos]
                raise SyntaxError(f"Unexpected character '{char}' at line {line}:{col}")

        # Always append EOF
        tokens.append(Token(TokenType.EOF, "", line, pos - col_start + 1))
        return tokens
