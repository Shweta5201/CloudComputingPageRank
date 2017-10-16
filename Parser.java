package parsing;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.omg.Messaging.SyncScopeHelper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Parser {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		final Pattern pattern = Pattern.compile("(?<=<target>)(.*?)(?=</target>)");//("<target>(.+?)</target>");
	
		File inputFile = new File(args[0]);
		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		String line = null;
		StringBuilder input = new StringBuilder();
		HashSet<String> titles = new HashSet<String>();
		while((line = reader.readLine())!= null){
			String[] components = line.split("\\t");
			titles.add(components[1]);
			//input.append(line);
			//System.out.println("title  is = " +components[1] );
		}
		reader.close();
		System.out.println("no of titles   " + titles.size());
		BufferedReader reader1 = new BufferedReader(new FileReader(inputFile));
		String line1 = null;
		HashSet<String> finalValues = new HashSet<>();
		//File output = new File("output.txt");
		PrintWriter f = new PrintWriter(new FileWriter("output.txt"));
		while((line1 = reader1.readLine())!= null){
			String[] components = line1.split("\\t"); 
			Matcher matcher = pattern.matcher(line1);
			//Str
			
//			while(matcher.find() && titles.contains(matcher.group())){
//	    		 // link.add(matcher.group());
//				 System.out.println(" group is " + matcher.group());
//				f.println(components[1]+"\t"+ matcher.group());
//	    	  } 
			
			
			while(matcher.find()) 
				{
					if(titles.contains(matcher.group())&& !(matcher.group().equals(components[1]))){
						
						//System.out.println(" group is " + matcher.group());
						String val = components[1]+"\t"+ matcher.group();
						//finalValues.add(val);
						f.println(components[1]+"\t"+ matcher.group());
				}
	    		 // link.add(matcher.group())
	    	  } 
			
		}
		
		
		reader1.close();
		
		
		
		
		
		
		
		
//		String contents = input.toString();
//		final Pattern pattern = Pattern.compile("(?<=<target>)(.*?)(?=</target>)");
//		Matcher matcher = pattern.matcher(contents);
//		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
//		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
//		Document document = documentBuilder.parse(contents);
//		document.getDocumentElement().normalize();
//		NodeList links = document.getElementsByTagName("target");
//		for(int i =0;i<links.getLength();i++){
//			Node node = links.item(i);
//			Element e = (Element) node;
//			if(node.getNodeType() == Node.ELEMENT_NODE)
//			System.out.println(e.getTextContent());
//		}

	}

}
