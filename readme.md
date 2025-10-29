# Data Pipeline for Handling Missing Values in Employee Data
## **Overview**

This project creates a robust data pipeline designed to handle missing values in employee data across different stages (Bronze, Silver, and Gold). It utilizes machine learning techniques to perform data imputation and provides aggregated insights at the final stage. The pipeline processes raw employee data, fills missing values, and stores the processed data in efficient formats for analysis.

### **Pipeline Stages:**

Bronze Store: Raw data with missing values is initially processed.

Silver Store: Machine learning models are used to impute missing values (age, salary, etc.) and encode categorical variables.

Gold Store: Aggregated insights such as the highest salary, minimum age, etc., are calculated for analysis.

### **Part-1 Focus:**

The focus of this phase is to process raw employee data, perform missing data imputation, and store the data in various formats (CSV, Parquet).

### **Requirements**

Ensure the following dependencies are installed to run the pipeline:

Python 3.7+

Libraries:
1. pandas
2. scikit-learn
3. numpy
4. watchdog

And other dependencies listed in requirements.txt

# **Pipeline Overview**
### **Part-1: Data Pipeline Stages**

### Bronze Store (Raw Data Processing):

The pipeline reads raw employee data in CSV format and processes missing values.

Handling Missing Values:

1. Age and Salary: Filled with -1.
2. Name: Filled with N/A.
3. Department: Categorical encoding applied.

### **Silver Store (Data Imputation with Machine Learning Models):**

Imputation Models: Utilizes Random Forest to impute missing values for:

1. Age
2. Salary
3. Number of Years Worked
4. Department (encoded categorically)
5. The model is trained using a train-test split for validation to ensure the accuracy of imputed values.

### **Gold Store (Aggregated Data):**

Aggregation at the department level, calculating:

1. Highest salary (with employee name)
2. Lowest salary (with employee name)
3. Minimum and Maximum age (with employee name)
4. Employee with the highest and lowest years of experience
5. The processed and aggregated data is saved in the Gold Store for final analysis.

### **Continuous Monitoring with Watchdog:**

A Watchdog service monitors the raw_store directory for new CSV files. When a new file is detected, it automatically triggers the pipeline to process the data, perform imputation, and generate aggregated results.

# **Limitations & Future Improvements (Part-2)**

Part-1 lays the groundwork for handling missing data and aggregation. Future enhancements will focus on:

Enhanced Model Accuracy: Explore alternative machine learning algorithms (e.g., XGBoost, SVM).

Real-Time Analytics: Integrate with Business Intelligence tools for real-time insights.

SQL Integration: Store processed data in an SQL database for more complex querying and reporting.

# **Conclusion**

This pipeline efficiently handles large employee datasets with missing values and produces useful aggregated insights for data analysis. With continuous monitoring, it’s easy to process new data files as they are added to the directory.