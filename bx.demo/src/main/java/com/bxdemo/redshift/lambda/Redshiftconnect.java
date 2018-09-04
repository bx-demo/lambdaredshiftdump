package com.bxdemo.redshift.lambda;

import com.bxdemo.redshift.lambda.Dumpcalls;

public class Redshiftconnect {
	public String calls() {
    	Dumpcalls dp = new Dumpcalls();
		System.out.println("Starting Unloading data");
		dp.create_connection();
		boolean clients_result = dp.clients();
		boolean contacts_result = dp.contacts();
		boolean contacts_addresses_result = dp.contact_addresses();
		boolean billing_payment_result = dp.billing_payments();
		boolean billing_codes_result = dp.billing_codes();
		boolean billing_entries_result = dp.billing_entries();
    	System.out.println("Finished Unloading data.");
    	if (clients_result &&  contacts_addresses_result && contacts_result && billing_codes_result && billing_entries_result && billing_payment_result )
    		return "Success";
    	else 
    		return "Failed";
     }
}
