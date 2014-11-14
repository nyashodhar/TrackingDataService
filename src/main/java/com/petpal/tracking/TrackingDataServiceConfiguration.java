package com.petpal.tracking;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@ComponentScan
@EnableAutoConfiguration
public class TrackingDataServiceConfiguration {

    @Value("${trackingService.executor.corePoolSize}")
    private int threadPoolExecutorCorePoolSize;

    @Value("${trackingService.executor.maxPoolSize}")
    private int threadPoolExecutorMaxPoolSize;

    @Value("${trackingService.executor.setQueueCapacity}")
    private int threadPoolExecutorQueueCapacity;

    public static void main(String[] args) {
        SpringApplication.run(TrackingDataServiceConfiguration.class, args);
    }

    @Bean(name = "threadPoolExecutor")
    public ThreadPoolTaskExecutor getThreadPoolExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(threadPoolExecutorCorePoolSize);
        threadPoolTaskExecutor.setMaxPoolSize(threadPoolExecutorMaxPoolSize);
        threadPoolTaskExecutor.setQueueCapacity(threadPoolExecutorQueueCapacity);
        threadPoolTaskExecutor.initialize();
        return threadPoolTaskExecutor;
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