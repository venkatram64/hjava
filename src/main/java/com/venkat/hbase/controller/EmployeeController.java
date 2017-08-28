package com.venkat.hbase.controller;

import com.venkat.hbase.common.Employee;
import com.venkat.hbase.service.EmployeeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

/**
 * Created by venkatram.veerareddy on 8/28/2017.
 */

@RestController
@RequestMapping("employee")
public class EmployeeController {
    @Autowired
    private EmployeeService employeeService;

    @RequestMapping("/hello")
    public String sayHello(){
        return "Hi Venkatram";
    }

    @RequestMapping(value="/employees", method = RequestMethod.GET, produces = APPLICATION_JSON_VALUE)
    public List<Employee> getAllEmployees(){
        return employeeService.getAllEmployees();
    }

    @RequestMapping(value="/createEmp", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ResponseBody
    public Employee addEmployee(@RequestBody Employee emp){
        return employeeService.addEmployee(emp);
    }

    @RequestMapping(value="/{empId}", method = RequestMethod.GET, produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<Employee> findEmployeeById(@PathVariable String empId){
        return ResponseEntity.ok().body(employeeService.getEmployee(empId));
    }

    @RequestMapping(value="/{empId}", method = RequestMethod.DELETE, produces = APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public ResponseEntity<String> deleteEmployeeById(@PathVariable String empId){
        boolean deleted = employeeService.deleteEmployee(empId);
        if(deleted) {
            return new ResponseEntity<String>("Unable to delete",HttpStatus.NO_CONTENT);
        }else{
            return new ResponseEntity<String>("Deleted", HttpStatus.NO_CONTENT);
        }
    }
}
