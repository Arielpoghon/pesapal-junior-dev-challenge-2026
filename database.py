"""
Core in-memory relational database engine.
Supports table creation, primary keys, and CRUD operations with basic WHERE filtering.
"""

import re
from typing import List, Dict, Any, Optional, Tuple


class Column:
    """Represents a column definition in a table."""

    def __init__(self, name: str, data_type: str, is_primary_key: bool = False):
        self.name = name
        self.data_type = data_type.upper()  # INTEGER, TEXT, REAL
        self.is_primary_key = is_primary_key

        if self.data_type not in ['INTEGER', 'TEXT', 'REAL']:
            raise ValueError(f"Unsupported data type: {data_type}")

    def __repr__(self):
        return f"Column({self.name}, {self.data_type})"


class Table:
    """Represents a table with schema and rows stored in-memory."""

    def __init__(self, name: str, columns: List[Column], primary_key: str = 'id'):
        self.name = name
        self.columns = columns
        self.column_map = {col.name: col for col in columns}
        self.primary_key = primary_key
        self.rows: List[Dict[str, Any]] = []
        self.next_id = 1

        # Verify primary key exists
        if primary_key not in self.column_map:
            raise ValueError(f"Primary key '{primary_key}' not found in columns")

    def insert(self, values: Dict[str, Any]) -> int:
        """
        Insert a row. Auto-generates primary key if not provided.
        Returns the ID of inserted row.
        """
        row = {}

        # Process each column
        for col in self.columns:
            if col.is_primary_key:
                # Auto-generate if not provided
                if col.name not in values:
                    row[col.name] = self.next_id
                else:
                    row[col.name] = values[col.name]
            else:
                if col.name not in values:
                    row[col.name] = None
                else:
                    row[col.name] = values[col.name]

        # Validate types
        self._validate_types(row)

        # Check primary key uniqueness
        pk_val = row[self.primary_key]
        for existing_row in self.rows:
            if existing_row[self.primary_key] == pk_val:
                raise ValueError(f"Primary key violation: {self.primary_key}={pk_val} already exists")

        # Add row
        self.rows.append(row)
        self.next_id += 1
        return pk_val

    def select(self, columns: Optional[List[str]] = None, where_clause: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Select rows. If columns is None, return all columns.
        where_clause: e.g., "age > 18 AND name LIKE 'John%'"
        """
        # Default to all columns
        if columns is None:
            columns = [col.name for col in self.columns]

        # Validate requested columns
        for col in columns:
            if col not in self.column_map:
                raise ValueError(f"Column '{col}' does not exist")

        # Filter rows based on WHERE clause
        filtered_rows = self.rows
        if where_clause:
            filtered_rows = [row for row in self.rows if self._evaluate_where(row, where_clause)]

        # Project requested columns
        result = []
        for row in filtered_rows:
            result.append({col: row[col] for col in columns})

        return result

    def update(self, values: Dict[str, Any], where_clause: Optional[str] = None) -> int:
        """
        Update rows matching WHERE clause.
        Returns count of updated rows.
        """
        # Validate column names
        for col_name in values:
            if col_name not in self.column_map:
                raise ValueError(f"Column '{col_name}' does not exist")

        # Validate types
        self._validate_types(values)

        # Find matching rows
        updated_count = 0
        for row in self.rows:
            if where_clause is None or self._evaluate_where(row, where_clause):
                row.update(values)
                updated_count += 1

        return updated_count

    def delete(self, where_clause: Optional[str] = None) -> int:
        """
        Delete rows matching WHERE clause.
        Returns count of deleted rows.
        """
        if where_clause is None:
            # Delete all rows
            count = len(self.rows)
            self.rows = []
            return count

        # Find rows to keep
        new_rows = [row for row in self.rows if not self._evaluate_where(row, where_clause)]
        deleted_count = len(self.rows) - len(new_rows)
        self.rows = new_rows
        return deleted_count

    def _validate_types(self, row: Dict[str, Any]):
        """Validate that values match their column types."""
        for col_name, value in row.items():
            if value is None:
                continue

            col = self.column_map.get(col_name)
            if not col:
                continue

            if col.data_type == 'INTEGER':
                if not isinstance(value, int):
                    raise ValueError(f"Column '{col_name}' expects INTEGER, got {type(value).__name__}")
            elif col.data_type == 'TEXT':
                if not isinstance(value, str):
                    raise ValueError(f"Column '{col_name}' expects TEXT, got {type(value).__name__}")
            elif col.data_type == 'REAL':
                if not isinstance(value, (int, float)):
                    raise ValueError(f"Column '{col_name}' expects REAL, got {type(value).__name__}")

    def _evaluate_where(self, row: Dict[str, Any], where_clause: str) -> bool:
        """
        Evaluate WHERE clause against a row.
        Supports: column = value, column > value, column < value, column LIKE pattern
        Supports AND operator for combining conditions.
        """
        # Split by AND (simple implementation)
        conditions = [c.strip() for c in where_clause.split(' AND ')]

        for condition in conditions:
            if not self._evaluate_condition(row, condition):
                return False

        return True

    def _evaluate_condition(self, row: Dict[str, Any], condition: str) -> bool:
        """Evaluate a single condition like 'age > 18' or 'name LIKE John%'"""
        condition = condition.strip()

        # LIKE operator
        if ' LIKE ' in condition:
            parts = condition.split(' LIKE ')
            if len(parts) != 2:
                raise ValueError(f"Invalid LIKE condition: {condition}")
            col_name = parts[0].strip()
            pattern = parts[1].strip().strip("'\"")

            if col_name not in row:
                raise ValueError(f"Column '{col_name}' does not exist")

            value = str(row[col_name])
            # Convert SQL LIKE pattern to Python regex
            regex_pattern = pattern.replace('%', '.*').replace('_', '.')
            return bool(re.match(f"^{regex_pattern}$", value))

        # Comparison operators: =, >, <
        for op in ['>=', '<=', '<>', '=', '>', '<']:
            if f' {op} ' in condition:
                parts = condition.split(f' {op} ')
                if len(parts) != 2:
                    raise ValueError(f"Invalid condition: {condition}")
                col_name = parts[0].strip()
                value_str = parts[1].strip().strip("'\"")

                if col_name not in row:
                    raise ValueError(f"Column '{col_name}' does not exist")

                row_value = row[col_name]

                # Parse value
                try:
                    if isinstance(row_value, int):
                        compare_value = int(value_str)
                    elif isinstance(row_value, float):
                        compare_value = float(value_str)
                    else:
                        compare_value = value_str
                except ValueError:
                    compare_value = value_str

                # Evaluate
                if op == '=':
                    return row_value == compare_value
                elif op == '>':
                    return row_value > compare_value
                elif op == '<':
                    return row_value < compare_value
                elif op == '>=':
                    return row_value >= compare_value
                elif op == '<=':
                    return row_value <= compare_value
                elif op == '<>':
                    return row_value != compare_value

        raise ValueError(f"Invalid condition: {condition}")


class Database:
    """Main database engine holding multiple tables."""

    def __init__(self):
        self.tables: Dict[str, Table] = {}

    def create_table(self, name: str, columns: List[Tuple[str, str]]) -> Table:
        """
        Create a table. First column is assumed to be primary key (INTEGER).
        columns: List of (name, type) tuples
        Example: [('id', 'INTEGER'), ('name', 'TEXT'), ('age', 'INTEGER')]
        """
        if name in self.tables:
            raise ValueError(f"Table '{name}' already exists")

        # Create Column objects
        col_objects = []
        for col_name, col_type in columns:
            is_pk = len(col_objects) == 0  # First column is PK
            col_objects.append(Column(col_name, col_type, is_primary_key=is_pk))

        # Create table
        pk_name = col_objects[0].name
        table = Table(name, col_objects, primary_key=pk_name)
        self.tables[name] = table
        return table

    def get_table(self, name: str) -> Optional[Table]:
        """Get a table by name."""
        return self.tables.get(name)

    def drop_table(self, name: str) -> bool:
        """Drop a table. Returns True if table existed."""
        if name in self.tables:
            del self.tables[name]
            return True
        return False

    def get_table_schema(self, name: str) -> str:
        """Get human-readable schema for a table."""
        table = self.get_table(name)
        if not table:
            return f"Table '{name}' does not exist"

        lines = [f"Table: {name}"]
        for col in table.columns:
            pk_marker = " (PRIMARY KEY)" if col.is_primary_key else ""
            lines.append(f"  {col.name}: {col.data_type}{pk_marker}")

        return "\n".join(lines)

    def list_tables(self) -> List[str]:
        """List all table names."""
        return list(self.tables.keys())
