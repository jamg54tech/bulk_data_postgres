package com.postgres.poc;

import com.postgres.poc.components.PostgressCopy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class Main implements CommandLineRunner {

    @Autowired
    PostgressCopy pc;

    public void getDate(){
        // Get the current date and time
        LocalDateTime currentDateTime = LocalDateTime.now();
        // Define a custom format pattern
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        // Format the current date and time using the custom pattern
        String formattedDateTime = currentDateTime.format(formatter);

        System.out.println("\n\nCurrent Date: " + formattedDateTime);
    }
    @Override
    public void run(String... args) throws Exception {

        //pc.loadData();


        System.out.println("\n INICIANDO....");

        this.getDate();

        /*
        System.out.println("\n INSERTANDO PROMOCIONES....");


        pc.loadData(
                "promotions",
                "C:\\Users\\JAMG54\\Desktop\\LIVERPOOL\\POCS\\PostgresCOPY\\PROMOTIONS\\liverpool_sku_promotion.txt.07032024"

        );
        */

        /*
        System.out.println("\n INSERTANDO SKUS....");

        pc.loadData(
                "skus",
                "C:\\Users\\JAMG54\\Desktop\\LIVERPOOL\\POCS\\PostgresCOPY\\PROMOTIONS\\lp_institutional_promotion.txt.07032024",
                new String[]{"sku","site_id","product_type","call_promo_service"}
        );
        */

        System.out.println("\n INSERTANDO RELACIONES....");

        pc.loadPromotionRelations(
                "sku_promo_relations",
                "C:\\Users\\JAMG54\\Desktop\\LIVERPOOL\\POCS\\PostgresCOPY\\PROMOTIONS\\lp_instnl_sku_promotion.txt.07032024"
        );

        this.getDate();


    }

}
