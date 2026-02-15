from flask import Flask, render_template, request, redirect, url_for, flash 
from database import Database

app = Flask(__name__)
app.secret_key = "super_secret_key_123"  # Required for flash messages
db = Database()

# Auto-create the students table
try:
    db.create_table("students", [ 
        ("id", "INTEGER"),
        ("name", "TEXT"),
        ("age", "INTEGER")
    ])
    print("Students table created or already exists.")
except Exception as e:
    print(f"Table creation skipped: {e}")

@app.route('/')
def index():
    print("Index route called")  # Debug
    try:
        table = db.get_table("students")
        if table is None:
            print("Students table not found!")
            students = []
        else:
            students = table.select()
            print(f"Loaded {len(students)} students")
    except Exception as e:
        print(f"Error loading students: {e}")
        students = []
    return render_template('index.html', students=students)

@app.route('/add', methods=['GET', 'POST'])
def add():
    if request.method == 'POST':
        name = request.form.get('name')
        age_str = request.form.get('age')
        if not name or not age_str:
            flash("Name and age are required!", "error")
            return render_template('add.html')
        try:
            age = int(age_str)
            db.get_table("students").insert({"name": name, "age": age})
            flash("Student added successfully!", "success")
            return redirect(url_for('index'))
        except Exception as e:
            flash(f"Error adding student: {str(e)}", "error")
    return render_template('add.html')

@app.route('/edit/<int:student_id>', methods=['GET', 'POST'])
def edit(student_id):
    try:
        table = db.get_table("students")
        if table is None:
            flash("Database error: Students table not found", "error")
            return redirect(url_for('index'))

        result = table.select(where=f"id = {student_id}")
        if not result:
            flash("Student not found", "error")
            return redirect(url_for('index'))
        student = result[0]

        if request.method == 'POST':
            name = request.form.get('name')
            age_str = request.form.get('age')
            if not name or not age_str:
                flash("Name and age are required!", "error")
                return render_template('edit.html', student=student)
            try:
                age = int(age_str)
                table.update({"name": name, "age": age}, f"id = {student_id}")  # positional where
                flash("Student updated successfully!", "success")
                return redirect(url_for('index'))
            except Exception as e:
                flash(f"Error updating student: {str(e)}", "error")
        return render_template('edit.html', student=student)
    except Exception as e:
        flash(f"Error: {str(e)}", "error")
        return redirect(url_for('index'))

@app.route('/delete/<int:student_id>', methods=['POST'])
def delete(student_id):
    try:
        table = db.get_table("students")
        if table is None:
            flash("Database error: Students table not found", "error")
            return redirect(url_for('index'))

        table.delete(f"id = {student_id}")  # FIXED: positional argument, no 'where='
        flash("Student deleted successfully!", "success")
    except Exception as e:
        flash(f"Error deleting student: {str(e)}", "error")
    return redirect(url_for('index'))

if __name__ == '__main__':
    print("Starting Flask app in debug mode...")
    app.run(debug=True)
