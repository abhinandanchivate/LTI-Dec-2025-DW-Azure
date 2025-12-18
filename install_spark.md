
## Part 1: Installing WSL/WSL2 on Windows

**Step 1: Check Windows Version**
- You need Windows 10 version 2004+ (Build 19041+) or Windows 11
- Check: Settings > System > About

**Step 2: Install WSL2**

Open **PowerShell** or **Command Prompt** as Administrator and run:

```powershell
wsl --install
```

This single command will:
- Enable WSL feature
- Install WSL2
- Download and install Ubuntu (default distribution)

**Step 3: Restart your computer**

**Step 4: Set up Ubuntu**
After restart, Ubuntu will open automatically:
- Create a username (lowercase recommended)
- Create a password (you won't see it while typing)

**Alternative: Manual Installation or Different Distro**

If `wsl --install` doesn't work:

```powershell
# Enable WSL
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart

# Enable Virtual Machine Platform
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart

# Restart computer, then set WSL2 as default
wsl --set-default-version 2
```

Install a distribution from Microsoft Store (Ubuntu 22.04 LTS recommended).

**Verify WSL2 installation:**
```powershell
wsl --list --verbose
```

You should see `VERSION 2` next to your distribution.

---

## Part 2: Installing PySpark in WSL2

Open WSL terminal (search "Ubuntu" in Windows Start menu):

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Java
sudo apt install openjdk-11-jdk -y

# Install Python and pip
sudo apt install python3 python3-pip python3-venv -y

# Install PySpark
pip3 install pyspark

# Configure environment
echo '
# Java Home
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin

# PySpark
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
' >> ~/.bashrc

# Reload configuration
source ~/.bashrc
```

---

## Part 3: Setting Up VSCode with WSL + PySpark

**Step 1: Install VSCode on Windows**
Download from: https://code.visualstudio.com/

**Step 2: Install Required Extensions**

Open VSCode and install:
1. **WSL** (by Microsoft) - Essential for WSL integration
2. **Python** (by Microsoft) - For Python support
3. **Pylance** (by Microsoft) - Python language server

**Step 3: Connect VSCode to WSL**

Method 1 - From VSCode:
- Press `F1` or `Ctrl+Shift+P`
- Type: `WSL: Connect to WSL`
- Select your distribution

Method 2 - From WSL terminal:
```bash
code .
```

This will install VSCode Server in WSL and open VSCode connected to WSL.

**Step 4: Set Up Python Environment in WSL**

Once connected to WSL (you'll see "WSL: Ubuntu" in bottom-left corner):

1. Create a project folder:
```bash
mkdir ~/pyspark-projects
cd ~/pyspark-projects
```

2. Create a virtual environment (optional but recommended):
```bash
python3 -m venv venv
source venv/bin/activate
pip install pyspark jupyter notebook
```

3. Open this folder in VSCode:
```bash
code .
```

**Step 5: Configure Python Interpreter**

In VSCode:
- Press `Ctrl+Shift+P`
- Type: `Python: Select Interpreter`
- Choose the interpreter (either system Python3 or your venv)

**Step 6: Create and Run PySpark Code**

Create a new file `test_spark.py`:

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("VSCode PySpark Test") \
    .master("local[*]") \
    .getOrCreate()

# Create sample data
data = [
    ("Alice", 25, "Engineering"),
    ("Bob", 30, "Sales"),
    ("Charlie", 35, "Engineering"),
    ("Diana", 28, "HR")
]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Age", "Department"])

# Show data
print("Original DataFrame:")
df.show()

# Perform some operations
print("\nEngineering Department:")
df.filter(df.Department == "Engineering").show()

print("\nAverage Age by Department:")
df.groupBy("Department").avg("Age").show()

# Stop Spark session
spark.stop()
```

**Step 7: Run the Script**

In VSCode terminal (make sure you're in WSL):
```bash
python3 test_spark.py
```

Or use the Run button (▶️) in the top-right corner.

---

## Part 4: Using Jupyter Notebooks (Optional)

**Install Jupyter:**
```bash
pip3 install jupyter notebook
```

**Configure for PySpark:**
```bash
pip3 install findspark
```

**Create a notebook:**
```bash
jupyter notebook
```

This will open in your browser. Create a new notebook and use:

```python
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Jupyter PySpark") \
    .master("local[*]") \
    .getOrCreate()

# Your PySpark code here
```

---

## Part 5: Useful VSCode Settings for PySpark

Create `.vscode/settings.json` in your project:

```json
{
    "python.defaultInterpreterPath": "/usr/bin/python3",
    "python.terminal.activateEnvironment": true,
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "files.autoSave": "afterDelay",
    "terminal.integrated.defaultProfile.linux": "bash"
}
```

---

## Part 6: Testing Your Setup

Create `complete_test.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# Initialize Spark
spark = SparkSession.builder \
    .appName("Complete Test") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")
print(f"Spark UI: http://localhost:4040")

# Create sample dataset
data = [
    (1, "John", 28, "IT", 75000),
    (2, "Jane", 32, "HR", 68000),
    (3, "Mike", 45, "IT", 95000),
    (4, "Sarah", 29, "Sales", 72000),
    (5, "Tom", 38, "IT", 88000)
]

df = spark.createDataFrame(data, ["id", "name", "age", "dept", "salary"])

print("\n=== DataFrame Schema ===")
df.printSchema()

print("\n=== Show Data ===")
df.show()

print("\n=== Department Statistics ===")
df.groupBy("dept") \
    .agg(
        count("*").alias("count"),
        avg("salary").alias("avg_salary"),
        avg("age").alias("avg_age")
    ).show()

spark.stop()
print("\nSpark session stopped successfully!")
```

Run it:
```bash
python3 complete_test.py
```

---

## Troubleshooting Common Issues

**1. "WSL 2 requires an update to its kernel component"**
- Download and install: https://aka.ms/wsl2kernel

**2. Java not found errors:**
```bash
# Find Java location
sudo update-alternatives --config java
# Use the path in your .bashrc
```

**3. VSCode can't find Python:**
- Make sure you're connected to WSL (check bottom-left corner)
- Reinstall Python extension while connected to WSL

**4. Permission denied errors:**
```bash
# Fix pip permissions
pip3 install --user pyspark
```

**5. Spark UI not accessible:**
- Access at: http://localhost:4040 (only while Spark is running)

---

You're now ready to develop PySpark applications in VSCode using WSL2! The setup gives you a Linux development environment directly in Windows with full IDE support.
