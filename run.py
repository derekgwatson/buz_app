# /run.py
from app import create_app

# Create app with development config
app = create_app('development')

if __name__ == "__main__":
    app.run(debug=True)