# [1378. Replace Employee ID With The Unique Identifier](https://leetcode.com/problems/replace-employee-id-with-the-unique-identifier?envType=study-plan-v2&envId=top-sql-50)

- ### Intuition
    The goal of this task is to display the unique ID of each employee, replacing the employee's `id` with the `unique_id` from the `EmployeeUNI` table. If no unique ID is available for an employee, we should return `null` for that employee's `unique_id`. This is essentially a **left join** operation between the `Employees` table and the `EmployeeUNI` table based on the employee `id`.

- ### Approach
    1. **Join Employees with EmployeeUNI**:
        - We need to combine the data from the two tables, where each employee's `id` from the `Employees` table matches the `id` from the `EmployeeUNI` table.
        - We will perform a **left join** operation. This ensures that all employees will appear in the result, and if there is no matching `unique_id` in `EmployeeUNI`, the result will show `null` for `unique_id`.

    2. **Select the Required Columns**:
        - After joining the tables, we only need the `unique_id` and `name` columns from the result. We will select these two columns to form the final output.

    3. **Handle Missing Data**:
        - For employees who don't have a corresponding entry in the `EmployeeUNI` table (i.e., they don’t have a `unique_id`), the result will show `null` in place of the `unique_id`.

    4. **Return the Final Result**:
        - The final result will include the `unique_id` (or `null` if missing) and the employee's `name`. This should be presented in any order as specified.

- ### Code Implementation
    - **SQL:**
        ```sql []
        SELECT e2.unique_id, e1.name FROM
        Employees e1 LEFT JOIN EmployeeUNI e2
        ON e1.id = e2.id    
        ```
    - **PySpark:**
        ```python3 []
        def replace_employee_id(employees: pyspark.sql.dataframe.DataFrame, 
                                employee_uni: pyspark.sql.dataframe.DataFrame) \
                    -> pyspark.sql.dataframe.DataFrame:
            output = employees.join(employee_uni, on = 'id', how = 'left')\
                                .select(['unique_id', 'name'])
            return output
        ```
    - **Pandas:**
        ```python3 []
        def replace_employee_id(employees: pd.DataFrame, 
                                employee_uni: pd.DataFrame) -> pd.DataFrame:
            output = employees.merge(employee_uni, on = 'id', how = 'left')
            output = output[['unique_id', 'name']]
            return output
        ```