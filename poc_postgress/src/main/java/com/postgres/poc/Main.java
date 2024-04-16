package com.postgres.poc;

import com.postgres.poc.components.PostgressCopy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

@Component
public class Main implements CommandLineRunner {

    @Autowired
    PostgressCopy pc;

    @Override
    public void run(String... args) throws Exception {

        //pc.loadData();


        System.out.println("\n INICIANDO....");
        System.out.println("\n\nCurrent Date: " + LocalDate.now());

        /*
        System.out.println("\n INSERTANDO PROMOCIONES....");


        pc.loadData(
                "promotions",
                "C:\\Users\\JAMG54\\Desktop\\LIVERPOOL\\POCS\\PostgresCOPY\\PROMOTIONS\\liverpool_sku_promotion.txt.07032024"

        );
        */

        System.out.println("\n INSERTANDO SKUS....");

        pc.loadData(
                "skus",
                "C:\\Users\\JAMG54\\Desktop\\LIVERPOOL\\POCS\\PostgresCOPY\\PROMOTIONS\\lp_institutional_promotion.txt.07032024",
                new String[]{"sku","site_id","product_type","call_promo_service"}
        );

        /*
        System.out.println("\n INSERTANDO RELACIONES....");

        pc.loadData(
                "sku_promo_relations",
                "C:\\Users\\JAMG54\\Desktop\\LIVERPOOL\\POCS\\PostgresCOPY\\PROMOTIONS\\lp_instnl_sku_promotion.txt.07032024"
        );

        System.out.println("\n\nCurrent Date: " + LocalDate.now());
        */

    }

}
