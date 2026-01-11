"""
Interactive SQL-like REPL for the simple in-memory database.
Allows users to create tables, insert data, query, update, and delete records.
"""

from database import Database, Column
from typing import List, Dict, Any


class DatabaseREPL:
    """Interactive command-line interface for the database."""

    def __init__(self):
        self.db = Database()
        self.running = True

    def run(self):
        """Main REPL loop."""
        print("\n" + "=" * 60)
        print("Simple In-Memory Relational Database")
        print("=" * 60)
        print("\nCommands:")
        print("  CREATE TABLE name (col1 TYPE, col2 TYPE, ...)")
        print("  INSERT INTO table (col1, col2) VALUES (val1, val2)")
        print("  SELECT col1, col2 FROM table WHERE condition")
        print("  UPDATE table SET col1=val1 WHERE condition")
        print("  DELETE FROM table WHERE condition")
        print("  SHOW TABLES")
        print("  SHOW SCHEMA table")
        print("  HELP")
        print("  EXIT")
        print("\nExample:")
        print("  CREATE TABLE students (id INTEGER, name TEXT, age INTEGER)")
        print("  INSERT INTO students (name, age) VALUES (Alice, 20)")
        print("  SELECT * FROM students WHERE age > 18")
        print("=" * 60 + "\n")

        while self.running:
            try:
                command = input("db> ").strip()
                if not command:
                    continue
                self.execute_command(command)
            except KeyboardInterrupt:
                print("\nExiting...")
                self.running = False
            except Exception as e:
                print(f"Error: {e}")

    def execute_command(self, command: str):
        """Parse and execute a command."""
        command = command.strip()
        if not command:
            return

        # Remove trailing semicolon
        if command.endswith(';'):
            command = command[:-1].strip()

        # Route to appropriate handler
        if command.upper().startswith('CREATE TABLE'):
            self.handle_create_table(command)
        elif command.upper().startswith('INSERT INTO'):
            self.handle_insert(command)
        elif command.upper().startswith('SELECT'):
            self.handle_select(command)
        elif command.upper().startswith('UPDATE'):
            self.handle_update(command)
        elif command.upper().startswith('DELETE FROM'):
            self.handle_delete(command)
        elif command.upper() == 'SHOW TABLES':
            self.handle_show_tables()
        elif command.upper().startswith('SHOW SCHEMA'):
            self.handle_show_schema(command)
        elif command.upper() in ['HELP', '?']:
            self.handle_help()
        elif command.upper() in ['EXIT', 'QUIT']:
            self.running = False
            print("Goodbye!")
        else:
            print(f"Unknown command: {command}")

    def handle_create_table(self, command: str):
        """Parse CREATE TABLE command."""
        # Format: CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
        try:
            # Extract table name and columns
            match_start = command.upper().index('CREATE TABLE') + len('CREATE TABLE')
            rest = command[match_start:].strip()

            # Find opening paren
            paren_idx = rest.index('(')
            table_name = rest[:paren_idx].strip()

            # Extract columns definition
            col_def = rest[paren_idx + 1 : rest.rindex(')')].strip()
            col_parts = [c.strip() for c in col_def.split(',')]

            columns = []
            for col_part in col_parts:
                parts = col_part.split()
                if len(parts) < 2:
                    raise ValueError(f"Invalid column definition: {col_part}")
                col_name = parts[0]
                col_type = parts[1].upper()
                columns.append((col_name, col_type))

            # Create table
            self.db.create_table(table_name, columns)
            print(f"Table '{table_name}' created successfully.")
            print(self.db.get_table_schema(table_name))

        except Exception as e:
            raise ValueError(f"Invalid CREATE TABLE syntax: {e}")

    def handle_insert(self, command: str):
        """Parse INSERT INTO command."""
        # Format: INSERT INTO table (col1, col2) VALUES (val1, val2)
        try:
            # Extract table name
            match_start = command.upper().index('INSERT INTO') + len('INSERT INTO')
            rest = command[match_start:].strip()

            paren_idx = rest.index('(')
            table_name = rest[:paren_idx].strip()

            table = self.db.get_table(table_name)
            if not table:
                raise ValueError(f"Table '{table_name}' does not exist")

            # Extract column names
            close_paren = rest.index(')')
            col_names_str = rest[paren_idx + 1 : close_paren]
            col_names = [c.strip() for c in col_names_str.split(',')]

            # Find VALUES
            values_idx = command.upper().index('VALUES')
            values_part = command[values_idx + len('VALUES'):].strip()

            # Extract values
            values_start = values_part.index('(')
            values_end = values_part.rindex(')')
            values_str = values_part[values_start + 1 : values_end]

            values = self._parse_values(values_str)

            if len(col_names) != len(values):
                raise ValueError(f"Column count ({len(col_names)}) does not match value count ({len(values)})")

            # Build row dict
            row_dict = {}
            for col_name, value in zip(col_names, values):
                # Try to convert to appropriate type
                col = table.column_map.get(col_name)
                if col:
                    if col.data_type == 'INTEGER':
                        row_dict[col_name] = int(value) if value is not None else None
                    elif col.data_type == 'REAL':
                        row_dict[col_name] = float(value) if value is not None else None
                    else:
                        row_dict[col_name] = value
                else:
                    row_dict[col_name] = value

            # Insert row
            row_id = table.insert(row_dict)
            print(f"Row inserted with ID: {row_id}")

        except Exception as e:
            raise ValueError(f"Invalid INSERT syntax: {e}")

    def handle_select(self, command: str):
        """Parse SELECT command."""
        # Format: SELECT col1, col2 FROM table WHERE condition
        try:
            # Extract table name
            from_idx = command.upper().index('FROM')
            select_part = command[7:from_idx].strip()  # Skip 'SELECT '

            # Parse columns
            if select_part == '*':
                columns = None
            else:
                columns = [c.strip() for c in select_part.split(',')]

            # Extract table name and WHERE clause
            rest = command[from_idx + 4:].strip()

            where_idx = rest.upper().find('WHERE')
            if where_idx != -1:
                table_name = rest[:where_idx].strip()
                where_clause = rest[where_idx + 5:].strip()
            else:
                table_name = rest.strip()
                where_clause = None

            table = self.db.get_table(table_name)
            if not table:
                raise ValueError(f"Table '{table_name}' does not exist")

            # Execute SELECT
            rows = table.select(columns, where_clause)

            # Display results
            if not rows:
                print("No rows found.")
            else:
                self._print_table(rows)

        except Exception as e:
            raise ValueError(f"Invalid SELECT syntax: {e}")

    def handle_update(self, command: str):
        """Parse UPDATE command."""
        # Format: UPDATE table SET col1=val1, col2=val2 WHERE condition
        try:
            # Extract table name
            match_start = command.upper().index('UPDATE') + len('UPDATE')
            rest = command[match_start:].strip()

            set_idx = rest.upper().index('SET')
            table_name = rest[:set_idx].strip()

            table = self.db.get_table(table_name)
            if not table:
                raise ValueError(f"Table '{table_name}' does not exist")

            # Extract SET clause
            rest = rest[set_idx + 3:].strip()
            where_idx = rest.upper().find('WHERE')

            if where_idx != -1:
                set_part = rest[:where_idx].strip()
                where_clause = rest[where_idx + 5:].strip()
            else:
                set_part = rest.strip()
                where_clause = None

            # Parse SET assignments
            assignments = [a.strip() for a in set_part.split(',')]
            update_dict = {}

            for assignment in assignments:
                eq_idx = assignment.index('=')
                col_name = assignment[:eq_idx].strip()
                value_str = assignment[eq_idx + 1:].strip()

                # Parse value
                value = self._parse_single_value(value_str)

                col = table.column_map.get(col_name)
                if col:
                    if col.data_type == 'INTEGER':
                        update_dict[col_name] = int(value) if value is not None else None
                    elif col.data_type == 'REAL':
                        update_dict[col_name] = float(value) if value is not None else None
                    else:
                        update_dict[col_name] = value
                else:
                    update_dict[col_name] = value

            # Execute UPDATE
            count = table.update(update_dict, where_clause)
            print(f"{count} row(s) updated.")

        except Exception as e:
            raise ValueError(f"Invalid UPDATE syntax: {e}")

    def handle_delete(self, command: str):
        """Parse DELETE FROM command."""
        # Format: DELETE FROM table WHERE condition
        try:
            match_start = command.upper().index('DELETE FROM') + len('DELETE FROM')
            rest = command[match_start:].strip()

            where_idx = rest.upper().find('WHERE')
            if where_idx != -1:
                table_name = rest[:where_idx].strip()
                where_clause = rest[where_idx + 5:].strip()
            else:
                table_name = rest.strip()
                where_clause = None

            table = self.db.get_table(table_name)
            if not table:
                raise ValueError(f"Table '{table_name}' does not exist")

            # Execute DELETE
            count = table.delete(where_clause)
            print(f"{count} row(s) deleted.")

        except Exception as e:
            raise ValueError(f"Invalid DELETE syntax: {e}")

    def handle_show_tables(self):
        """Show all tables."""
        tables = self.db.list_tables()
        if not tables:
            print("No tables created yet.")
        else:
            print("Tables:")
            for table_name in tables:
                print(f"  - {table_name}")

    def handle_show_schema(self, command: str):
        """Show table schema."""
        try:
            table_name = command.split('SHOW SCHEMA')[1].strip().upper()
            print(self.db.get_table_schema(table_name))
        except Exception as e:
            raise ValueError(f"Invalid SHOW SCHEMA syntax: {e}")

    def handle_help(self):
        """Show help message."""
        print("\nAvailable Commands:")
        print("\n1. CREATE TABLE")
        print("   CREATE TABLE name (col1 TYPE, col2 TYPE, ...)")
        print("   Types: INTEGER, TEXT, REAL")
        print("   First column is automatically the primary key")
        print("   Example: CREATE TABLE users (id INTEGER, name TEXT, email TEXT)")

        print("\n2. INSERT")
        print("   INSERT INTO table (col1, col2) VALUES (val1, val2)")
        print("   Example: INSERT INTO users (name, email) VALUES (Alice, alice@example.com)")

        print("\n3. SELECT")
        print("   SELECT col1, col2 FROM table [WHERE condition]")
        print("   SELECT * FROM table")
        print("   Conditions: col=val, col>val, col<val, col LIKE pattern")
        print("   AND is supported for multiple conditions")
        print("   Example: SELECT * FROM users WHERE age > 18 AND name LIKE 'A%'")

        print("\n4. UPDATE")
        print("   UPDATE table SET col1=val1, col2=val2 [WHERE condition]")
        print("   Example: UPDATE users SET email='newemail@example.com' WHERE name='Alice'")

        print("\n5. DELETE")
        print("   DELETE FROM table [WHERE condition]")
        print("   Example: DELETE FROM users WHERE name='Bob'")

        print("\n6. SHOW TABLES")
        print("   List all tables")

        print("\n7. SHOW SCHEMA table")
        print("   Show table structure")

        print("\n")

    def _parse_values(self, values_str: str) -> List[Any]:
        """Parse comma-separated values, handling quoted strings."""
        values = []
        current = ""
        in_quotes = False
        quote_char = None

        for char in values_str:
            if char in ['"', "'"] and not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char and in_quotes:
                in_quotes = False
            elif char == ',' and not in_quotes:
                values.append(self._parse_single_value(current.strip()))
                current = ""
                continue

            current += char

        if current.strip():
            values.append(self._parse_single_value(current.strip()))

        return values

    def _parse_single_value(self, value_str: str) -> Any:
        """Parse a single value, handling types and quotes."""
        value_str = value_str.strip()

        # Remove quotes if present
        if (value_str.startswith('"') and value_str.endswith('"')) or \
           (value_str.startswith("'") and value_str.endswith("'")):
            return value_str[1:-1]

        # Try to parse as number
        try:
            if '.' in value_str:
                return float(value_str)
            else:
                return int(value_str)
        except ValueError:
            return value_str

    def _print_table(self, rows: List[Dict[str, Any]]):
        """Print rows in a nice table format."""
        if not rows:
            return

        # Get all columns
        columns = list(rows[0].keys())

        # Calculate column widths
        col_widths = {col: len(str(col)) for col in columns}
        for row in rows:
            for col in columns:
                col_widths[col] = max(col_widths[col], len(str(row[col])))

        # Print header
        header = " | ".join(str(col).ljust(col_widths[col]) for col in columns)
        print(header)
        print("-" * len(header))

        # Print rows
        for row in rows:
            print(" | ".join(str(row[col]).ljust(col_widths[col]) for col in columns))

        print(f"\n({len(rows)} row(s))")


def main():
    """Start the REPL."""
    repl = DatabaseREPL()
    repl.run()


if __name__ == '__main__':
    main()
