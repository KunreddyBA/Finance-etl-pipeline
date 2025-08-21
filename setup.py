#!/usr/bin/env python3
"""
Setup script for LendingClub ETL Pipeline
Automates the initial project setup process.
"""

import os
import sys
import subprocess
import platform

def run_command(command, description):
    """Run a shell command and handle errors."""
    print(f"\n{description}...")
    try:
        # Use list format for commands with paths to handle spaces properly
        if isinstance(command, str):
            result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        else:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"{description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"{description} failed: {e}")
        print(f"Error output: {e.stderr}")
        return False

def check_python_version():
    """Check if Python version is compatible."""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("Python 3.8 or higher is required")
        return False
    print(f"Python {version.major}.{version.minor}.{version.micro} detected")
    return True

def setup_virtual_environment():
    """Create and activate virtual environment."""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    venv_path = os.path.join(script_dir, "venv")
    
    if os.path.exists(venv_path):
        print("Virtual environment already exists")
        return True
    
    # Change to the script directory before creating venv
    original_cwd = os.getcwd()
    os.chdir(script_dir)
    
    try:
        if platform.system() == "Windows":
            success = run_command("python -m venv venv", "Creating virtual environment")
        else:
            success = run_command("python3 -m venv venv", "Creating virtual environment")
        return success
    finally:
        os.chdir(original_cwd)

def install_dependencies():
    """Install required dependencies."""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    requirements_path = os.path.join(script_dir, "requirements.txt")
    venv_pip_cmd = os.path.join(script_dir, "venv", "Scripts", "pip.exe") if platform.system() == "Windows" else os.path.join(script_dir, "venv", "bin", "pip")
    
    # Install dependencies using list format to handle paths with spaces
    command = [venv_pip_cmd, "install", "-r", requirements_path]
    return run_command(command, "Installing dependencies")

def create_sample_data():
    """Create sample data for testing."""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    venv_python_cmd = os.path.join(script_dir, "venv", "Scripts", "python.exe") if platform.system() == "Windows" else os.path.join(script_dir, "venv", "bin", "python")
    sample_data_script = os.path.join(script_dir, "scripts", "create_sample_data.py")
    
    # Use list format to handle paths with spaces
    command = [venv_python_cmd, sample_data_script]
    return run_command(command, "Creating sample data")

def main():
    """Main setup function."""
    print("Setting up LendingClub ETL Pipeline...")
    print("=" * 50)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Setup virtual environment
    if not setup_virtual_environment():
        print("Failed to create virtual environment")
        sys.exit(1)
    
    # Install dependencies
    if not install_dependencies():
        print("Failed to install dependencies")
        sys.exit(1)
    
    # Create sample data
    if not create_sample_data():
        print("Failed to create sample data")
        sys.exit(1)
    
    print("\n" + "=" * 50)
    print("Setup completed successfully!")
    print("\nNext steps:")
    print("1. Activate virtual environment:")
    if platform.system() == "Windows":
        print("   .\\venv\\Scripts\\Activate.ps1")
    else:
        print("   source venv/bin/activate")
    print("2. Run the ETL pipeline:")
    print("   python scripts/etl_pipeline_pandas.py")
    print("3. Run the analysis:")
    print("   python scripts/analyze.py")
    print("\nFor more information, see README.md")

if __name__ == "__main__":
    main()
