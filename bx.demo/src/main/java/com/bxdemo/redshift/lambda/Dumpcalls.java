package com.bxdemo.redshift.lambda;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Dumpcalls {
	static Statement stmt = null;
	static Connection conn = null;

	public boolean create_connection() {
	     String dbURL = null;
	   	 String MasterUsername = null;
	   	 String MasterUserPassword = null;         
	            dbURL = System.getenv("dbURL");
	            MasterUsername = System.getenv("MasterUsername");
	            MasterUserPassword = System.getenv("MasterUserPassword");	     
	        try{
	           
	          // Class.forName("com.amazon.redshift.jd bc.Driver");
	           System.out.println("Connecting to database...");
	           Properties props = new Properties();
	           props.setProperty("user", MasterUsername);
	           props.setProperty("password", MasterUserPassword);
	           conn = DriverManager.getConnection(dbURL, props);
	           return true;
	        }
	        catch(Exception ex){
	            ex.printStackTrace();
	            System.exit(-1);
	        }
	        return false;
	}
    public String getdate() {
    	SimpleDateFormat formatter = new SimpleDateFormat("MMddyyyy");
    	Date date = new Date();
    	String date_cur = formatter.format(date);
    	return date_cur;
    	
    }
	public boolean clients() {
    	try {
    		System.out.println("Unloading Client table");
    		stmt = conn.createStatement();
    		String sql;
    		sql = "unload('select * from customers.clients order by clientid ') to"
    				+ " 's3://redshift.dump/customers.clients/ "+getdate()+"' "
    				+ " DELIMITER AS ','"
    				+ " ADDQUOTES " 
    				+ " NULL AS ''"
    				+ " credentials  'aws_access_key_id="+System.getenv("access_key")+";aws_secret_access_key="+System.getenv("secret_key")+"' "
    				+ " ALLOWOVERWRITE "
    				+ " parallel off " 
    				+ ";";           
    		int i = stmt.executeUpdate(sql);
    		System.out.println("Number of rows effected: customers.clients :  "+i);
    		String sql2 = "commit;";
    		stmt.executeUpdate(sql2);
    		stmt.close();
    		System.out.println("Finished Unloading client table");
    	}
    	catch(Exception ex) {
    		ex.printStackTrace();
    		System.exit(-1);
    		return false;
    	}
    	return true;
    }
	public boolean contact_addresses() {
    	try {
    		System.out.println("Unloading contact_addresses table");
    		stmt = conn.createStatement();
    		String sql;
    		sql = "unload('select * from customers.contact_addresses order by id ') to"
    				+ " 's3://redshift.dump/customers.contact.addresses/ "+getdate()+"' "
    				+ " DELIMITER AS ','"
    				+ " ADDQUOTES " 
    				+ " NULL AS ''"
    				+ " credentials  'aws_access_key_id="+System.getenv("access_key")+";aws_secret_access_key="+System.getenv("secret_key")+"' "
    				+ " ALLOWOVERWRITE "
    				+ " parallel off " 
    				+ ";";           
    		int i = stmt.executeUpdate(sql);
    		System.out.println("Number of rows effected: customers.contact_addresses :  "+i);
    		String sql2 = "commit;";
    		stmt.executeUpdate(sql2);
    		stmt.close();
    		System.out.println("Finished Unloading contact_addresses table");
    	}
    	catch(Exception ex) {
    		ex.printStackTrace();
    		System.exit(-1);
    		return false;
    	}
    	return true;
    }
	public boolean contacts() {
    	try {
    		System.out.println("Unloading contacts table");
    		stmt = conn.createStatement();
    		String sql;
    		sql = "unload('select * from customers.contacts order by id ') to"
    				+ " 's3://redshift.dump/customers.contacts/ "+getdate()+"' "
    				+ " DELIMITER AS ','"
    				+ " ADDQUOTES " 
    				+ " NULL AS ''"
    				+ " credentials  'aws_access_key_id="+System.getenv("access_key")+";aws_secret_access_key="+System.getenv("secret_key")+"' "
    				+ " ALLOWOVERWRITE "
    				+ " parallel off " 
    				+ ";";           
    		int i = stmt.executeUpdate(sql);
    		System.out.println("Number of rows effected: customers.contacts :  "+i);
    		String sql2 = "commit;";
    		stmt.executeUpdate(sql2);
    		stmt.close();
    		System.out.println("Finished Unloading contacts table");
    	}
    	catch(Exception ex) {
    		ex.printStackTrace();
    		System.exit(-1);
    		return false;
    	}
    	return true;
    }
	public boolean billing_codes() {
    	try {
    		System.out.println("Unloading billing_codes table");
    		stmt = conn.createStatement();
    		String sql;
    		sql = "unload('select * from customers.billing_codes order by id ') to"
    				+ " 's3://redshift.dump/customers.billing.codes/ "+getdate()+"' "
    				+ " DELIMITER AS ','"
    				+ " ADDQUOTES " 
    				+ " NULL AS ''"
    				+ " credentials  'aws_access_key_id="+System.getenv("access_key")+";aws_secret_access_key="+System.getenv("secret_key")+"' "
    				+ " ALLOWOVERWRITE "
    				+ " parallel off " 
    				+ ";";           
    		int i = stmt.executeUpdate(sql);
    		System.out.println("Number of rows effected: customers.billing_codes :  "+i);
    		String sql2 = "commit;";
    		stmt.executeUpdate(sql2);
    		stmt.close();
    		System.out.println("Finished Unloading billing_codes table");
    	}
    	catch(Exception ex) {
    		ex.printStackTrace();
    		System.exit(-1);
    		return false;
    	}
    	return true;
    }
	public boolean billing_entries() {
    	try {
    		System.out.println("Unloading billing_entries table");
    		stmt = conn.createStatement();
    		String sql;
    		sql = "unload('select * from customers.billing_entries order by id ') to"
    				+ " 's3://redshift.dump/customers.billing.entries/ "+getdate()+"' "
    				+ " DELIMITER AS ','"
    				+ " ADDQUOTES " 
    				+ " NULL AS ''"
    				+ " credentials  'aws_access_key_id="+System.getenv("access_key")+";aws_secret_access_key="+System.getenv("secret_key")+"' "
    				+ " ALLOWOVERWRITE "
    				+ " parallel off " 
    				+ ";";           
    		int i = stmt.executeUpdate(sql);
    		System.out.println("Number of rows effected: customers.billing_entries :  "+i);
    		String sql2 = "commit;";
    		stmt.executeUpdate(sql2);
    		stmt.close();
    		System.out.println("Finished Unloading billing_entries table");
    	}
    	catch(Exception ex) {
    		ex.printStackTrace();
    		System.exit(-1);
    		return false;
    	}
    	return true;
    }
	public boolean billing_payments() {
    	try {
    		System.out.println("Unloading billing_payments table");
    		stmt = conn.createStatement();
    		String sql;
    		sql = "unload('select * from customers.billing_payments order by id ') to"
    				+ " 's3://redshift.dump/customers.billing.payments/ "+getdate()+"' "
    				+ " DELIMITER AS ','"
    				+ " ADDQUOTES " 
    				+ " NULL AS ''"
    				+ " credentials  'aws_access_key_id="+System.getenv("access_key")+";aws_secret_access_key="+System.getenv("secret_key")+"' "
    				+ " ALLOWOVERWRITE "
    				+ " parallel off " 
    				+ ";";           
    		int i = stmt.executeUpdate(sql);
    		System.out.println("Number of rows effected: customers.billing_payments :  "+i);
    		String sql2 = "commit;";
    		stmt.executeUpdate(sql2);
    		stmt.close();
    		System.out.println("Finished Unloading billing_payments table");
    	}
    	catch(Exception ex) {
    		ex.printStackTrace();
    		System.exit(-1);
    		return false;
    	}
    	return true;
    }


}
