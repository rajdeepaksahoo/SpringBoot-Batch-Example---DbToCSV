package com.onlinetutorialspoint.config;

import com.onlinetutorialspoint.listener.JobListener;
import com.onlinetutorialspoint.model.Employee;
import com.onlinetutorialspoint.model.EmployeeDTO;
import com.onlinetutorialspoint.processor.EmployeeProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {
    @Autowired
    JobBuilderFactory jobBuilderFactory;
    @Autowired
    StepBuilderFactory stepBuilderFactory;
    @Autowired
    DataSource dataSource;

    @Bean
    public JdbcCursorItemReader<Employee> reader() {
        JdbcCursorItemReader<Employee> jdbcCursorItemReader = new JdbcCursorItemReader<>();
        jdbcCursorItemReader.setDataSource(dataSource);
        jdbcCursorItemReader.setSql("SELECT first_name, last_name, company_name, address, city, county, state, zip FROM employee");
        jdbcCursorItemReader.setRowMapper((resultSet, rowNum) -> {
            Employee user = new Employee();
            user.setFirstName(resultSet.getString("first_name"));
            user.setLastName(resultSet.getString("last_name"));
            user.setCompanyName(resultSet.getString("company_name"));
            user.setAddress(resultSet.getString("address"));
            user.setCity(resultSet.getString("city"));
            user.setCounty(resultSet.getString("county"));
            user.setState(resultSet.getString("state"));
            user.setZip(resultSet.getString("zip"));
            return user;
        });
        return jdbcCursorItemReader;
    }

    @Bean
    public EmployeeProcessor userProcessor() {
        return new EmployeeProcessor();
    }

    @Bean
    public FlatFileItemWriter<EmployeeDTO> writer() {
        FlatFileItemWriter<EmployeeDTO> fileItemWriter = new FlatFileItemWriter<>();
        fileItemWriter.setResource(new FileSystemResource("abc.csv"));

        fileItemWriter.setLineAggregator(new DelimitedLineAggregator() {{
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor() {{
                setNames(new String[]{"firstName", "lastName", "companyName", "address", "city", "county", "state", "zip"});
            }});
        }});
        return fileItemWriter;
    }

    @Bean
    public Step step1(ItemReader<Employee> reader, ItemProcessor<Employee, EmployeeDTO> processor, ItemWriter<EmployeeDTO> writer) {
        return stepBuilderFactory.get("step1")
                .<Employee, EmployeeDTO>chunk(10)
                .reader(reader())
                .processor(processor)
                .writer(writer())
                .build();
    }

    @Bean
    public Job importUserJob(JobListener listener, Step step1) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }
}
