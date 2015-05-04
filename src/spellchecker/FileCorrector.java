package spellchecker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import BerkeleyDB.DBWrapper;

public class FileCorrector extends Corrector {
	File correctFile;
	FileReader freader;
	BufferedReader breader;

	public FileCorrector(String filePath){
		correctFile = new File(filePath);
	}
	
	public FileCorrector(){
		
	}
	
	public void writeToDB(){
		DBWrapper db = new DBWrapper(DBdir.dir);

		Set<String> results = new HashSet<String>();
		String line;
		try {
			freader= new FileReader(correctFile);
			breader = new BufferedReader(freader);
			
			while((line=breader.readLine()) != null){
				String[] pairs = line.split(",");
				db.addMisspell(pairs[0], pairs[1]);
				
			}
			breader.close();
			freader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		db.closeEnv();
	}
	
	public boolean containsMisspell(String wrong){
		DBWrapper db = new DBWrapper(DBdir.dir);
		boolean contain = db.containsMisspell(wrong);
		db.closeEnv();
		return contain;
	}
	
	public String getCorrection(String word){
		DBWrapper db = new DBWrapper(DBdir.dir);
		String right = db.getRight(word);
		db.closeEnv();
		return right;
		
	}

	@Override
	public Set<String> getCorrections(String wrong) {
		Set<String> results = new HashSet<String>();
		String line;
		try {
			freader= new FileReader(correctFile);
			breader = new BufferedReader(freader);
			
			while((line=breader.readLine()) != null){
				String[] pairs = line.split(",");
				if(pairs[0].equals(wrong.toLowerCase())){
					results.add(pairs[1]);
					
				}
				
			}
			breader.close();
			freader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Set<String> fin = matchCase(wrong, results);
		
		return fin;
	}

}
