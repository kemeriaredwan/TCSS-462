
package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 *
 * @author betelhem
 */
public class Transform implements RequestHandler<Request, HashMap<String, Object>>{
    String filename ="";
    String bucketname="";
    int orderID = 6;
    boolean duplicated = false;
    
      public HashMap<String, Object> handleRequest(Request request, Context context){
          
          Inspector inspector = new Inspector();
          inspector.inspectAll();
          
          AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();           
          filename= request.getFilename();
          bucketname= request.getBucketname();
          
          //get object file using source bucket and srcKey name
          S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename)); 
          
          //get content of the file
          InputStream objectData = s3Object.getObjectContent(); 
          try{
             dataInputStream(objectData);
        }
          catch(IOException ex){
              //System.out.println("File not found",null,ex);
          }
          
      
          return inspector.finish();
      }

    public void dataInputStream(InputStream input) throws IOException {
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        List<List<String>> list = new ArrayList<>();
        String comma;
        String row;
        
        while((row = reader.readLine()) != null){
            String[] value = row.split(",");
            list.add(Arrays.asList(value));
        }
        transformData(list);
        writeCSV(list);
    }

    public List<List<String>> transformData(List<List<String>> list){

       list = removeDuplicates(list);
       return list;
    }
    public List<List<String>> removeDuplicates(List<List<String>> list){
        List<List<String>> newlist = new ArrayList<>(list.size());
        list.forEach((iteration)->{
            
            
            String id1 = iteration.get(orderID);
            
            for(List<String> newIterator : newlist){
                String id2 = newIterator.get(orderID);
                if(id1.equals(id2)){
                    duplicated = true;
                    
                }
            }
            if(duplicated != true){
                newlist.add(iteration);
                
            }
        
    });
        
        return newlist;
    }
    
    public void writeCSV(List<List<String>> list){
        StringWriter sw = new StringWriter();
           int i=0; 
          for(List<String> row: list) {
            for (String value: row) {
                sw.append(value);
                if(i++ != row.size() - 1)
                    sw.append(',');
            }
            sw.append("\n");            
        }
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");
        
        // Create new file on S3
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.putObject(bucketname, "test.csv", is, meta);
        
         
    }

}
           
  
    

  
