package com.petpal.tracking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
@EnableAutoConfiguration
public class TrackingDataServiceConfiguration {

    public static void main(String[] args) {
        SpringApplication.run(TrackingDataServiceConfiguration.class, args);
    }

    //@Bean
    //public CustomEditorRegistrar customEditorRegistrar() {
    //    return new CustomEditorRegistrar();
    //}

    //@Bean
    //public CustomDateEditorRegistrar customDateEditorRegistrar() {
    //    return new CustomDateEditorRegistrar();
    //}

    //@Bean
    //public CustomEditorConfigurer customEditorConfigurer() {
    //
    //    PropertyEditorRegistrar[] propertyEditorRegistrars = {customEditorRegistrar(), customDateEditorRegistrar()};
    //
    //    CustomEditorConfigurer customEditorConfigurer = new CustomEditorConfigurer();
    //    customEditorConfigurer.setPropertyEditorRegistrars(propertyEditorRegistrars);
    //
    //    return customEditorConfigurer;
    //}

    //@Bean
    //public CloseableHttpClient httpClient() {
    //    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    //    httpClientBuilder.setMaxConnPerRoute(httpClientMaxConnectionsPerRoute);
    //    httpClientBuilder.setMaxConnTotal(httpClientMaxConnectionsTotal);
    //    CloseableHttpClient closeableHttpClient = httpClientBuilder.build();
    //    return closeableHttpClient;
    //}

    /*

    public class CustomDateEditorRegistrar implements PropertyEditorRegistrar {

        @Override
        public void registerCustomEditors(PropertyEditorRegistry registry) {
            registry.registerCustomEditor(Date.class, new DateEditor());
        }
    }

    */

    /*

    public class CustomEditorRegistrar implements PropertyEditorRegistrar {

        @Override
        public void registerCustomEditors(PropertyEditorRegistry registry) {
            registry.registerCustomEditor(TrackingMetricsSet.class, new TrackingMetricsSetEditor());
            //registry.registerCustomEditor(Date.class, new DateEditor());
        }
    }

    */

}