package SparkSQLWithHive;

import org.apache.spark.sql.SparkSession;

public class QueryHiveDataWithSparkSQL
{
	 public static void main(String[] args) {
		 
		 SparkSession spark = SparkSession.builder().master("local[*]")
		    .appName("Query Hive Data with Spark SQL")
		    .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
		    .config("spark.sql.warehouse.dir","/users/hive/warehouse")
		    .enableHiveSupport()
		    .getOrCreate();

		 System.out.println("Welcome to BDT Project Part # 2 - Spark SQL and Hive Togather!!!");
		 
		 System.out.println("Query Analysis on Chicago's Employees Data:");
		 
		 /**
		  *  NOTE: Skipping the header row while querying explicitly as spark SQL is not taking the hive table properties of skipping header row and OPTION function is not working!
		  */
		
		 // QUERY AS CODE EXAMPLE
		 spark.read()
		      .option("header", "false") // not functional here
		      .table("employees_view")
		      .filter("name != 'Name'")
		      .limit(5)
		      .show();
		  System.out.println("Analysis # 0: SPARK SQL (HIVE) QUERY AS CODE !");
			 
		 
		 // Analysis # 1
		 spark.sql("SELECT department DEPARTMENT,"
		 		+ " ROUND(SUM(COALESCE(typical_hours, 0)*COALESCE(hourly_rate, 0)*4*12 + COALESCE(anual_salary,0)),2) ANNUAL_DEPT_EXPENSE_ON_SALARIES"
		 		+ " FROM employees_view"
		 		+ " WHERE name != 'Name'"
		 		+ " GROUP BY department"
		 		+ " ORDER BY department LIMIT 5;").show();
		 System.out.println("Analysis # 1: How much each department spend on employees salary annually (top 10 results).");
		 
		 // Analysis # 2
		 spark.sql("SELECT job_title POSITION,"
		 		+ " department DEPARTMENT,"
		 		+ " ROUND(AVG(anual_salary),2) AVG_ANNUAL_SALARY"
		 		+ " FROM employees_view"
		 		+ " WHERE name != 'Name' AND job_type = 'F' AND salary_type = 'Salary'"
		 		+ " GROUP BY job_title, department"
		 		+ " ORDER BY AVG_ANNUAL_SALARY LIMIT 5;").show();
		 System.out.println("Analysis # 2: Average Salary paid in each department with respect to available positions. (5 results only)");
		 
		 // Analysis # 3
	     spark.sql("SELECT job_title JOB_TITLE,"
		 		+ " ROUND(AVG(typical_hours*4),2) AVERAGE_MONTHLY_HOURS,"
		 		+ " ROUND(AVG(typical_hours*hourly_rate*4),2) AVERAGE_MONTHLY_SALARY"
		 		+ " FROM employees_view"
		 		+ " WHERE name != 'Name' AND job_type = 'P' AND salary_type = 'Hourly'"
		 		+ " GROUP BY job_title"
		 		+ " ORDER BY AVERAGE_MONTHLY_HOURS DESC LIMIT 3;").show();
	     System.out.println("Analysis # 3: Average monthly hours and salary of Hourly employees doing part-time job in chicago w.r.t to their job titles (top 3 results only).");
		 
	     // Analysis # 4
		 spark.sql("SELECT job_type JOB_TYPE,"
		 		+ " ROUND(AVG(hourly_rate),2) AS AVG_HOURLY_RATE"
		 		+ " FROM employees_view"
		 		+ " WHERE name != 'Name' AND salary_type = 'Hourly'"
		 		+ " GROUP BY job_type;").show();
		 System.out.println("Analysis # 4: Average hourly rate for full-time vs part-time emps.");
		 
		 // Analysis # 5
	     spark.sql("SELECT department DEPARTMENT,"
		 		+ " ROUND(MIN(anual_salary),2) AS MIN_SALARY,"
		 		+ " ROUND(AVG(anual_salary),2) AS AVG_SALARY,"
		 		+ " ROUND(MAX(anual_salary),2) AS MAX_SALARY"
		 		+ " FROM employees_view"
		 		+ " WHERE name != 'Name'"
		 		+ " GROUP BY department"
		 		+ " ORDER BY MIN_SALARY ASC LIMIT 3").show();
		 System.out.println("Analysis # 5: Department with the Minimum, Average, and Maximum Salaries. (show 3 departments only).");
	
		 //spark session closed
		 spark.stop();
	 }
	
}
