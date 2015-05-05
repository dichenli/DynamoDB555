/**
 * 
 */
package Utils;

import java.io.*;
//import java.io.StringWriter;
//import java.net.Socket;
import java.nio.file.Files;
import java.util.Scanner;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;


// TODO: Auto-generated Javadoc
/**
 * The Class IOUtils.
 *
 * @author dichenli
 * static methods to facilitates IO classes
 */
public class IOUtils {
	
	/**
	 * count number of lines of a file using its scanner to scan line by line
	 * return number of lines. Return null if input file is illegal. No exception thrown.
	 *
	 * @param file the file
	 * @return the long
	 */
	public static Long countLines(File file) {
		if(!fileExists(file)) {
			return null;
		}
		Scanner sc = getScanner(file);
		long count = 0;
		while(sc.hasNextLine()) {
			sc.nextLine();
			count++;
		}
		sc.close();
		return count;
	}

	/**
	 * Gets the reader.
	 *
	 * @param is the is
	 * @return the reader
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static BufferedReader getReader(
			InputStream is) throws IOException {
		if(is == null) {
			throw new IOException();
		}
		InputStreamReader isr = null;
		BufferedReader in = null;
		isr = new InputStreamReader(is);
		in = new BufferedReader(isr);
		return in;
	}
	
	/**
	 * open an scanner to a file, no exception thrown, return null for abnormals.
	 *
	 * @param file the file
	 * @return the scanner
	 */
	public static Scanner getScanner(File file) {
		if(file == null || !file.exists() || file.isDirectory()) {
			return null;
		}
		try {
			Scanner sc = new Scanner(file);
			return sc;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * suitable for writing characters.
	 *
	 * @param os the os
	 * @return the writer
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static PrintWriter getWriter(
			OutputStream os) throws IOException {
		if(os == null) {
			throw new IOException();
		}
		
		PrintWriter writer = null;
		writer = new PrintWriter(os);
		return writer;
	}
	
	/**
	 * get the printwriter of a file, no null return, but throws IOException.
	 *
	 * @param file the file
	 * @return the writer
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static PrintWriter getWriter(File file) throws IOException {
		OutputStream out = new FileOutputStream(file);
		return getWriter(out);
	}
	
	/**
	 * is a valid file, exists, and not direcotry, not null.
	 *
	 * @param file the file
	 * @return true, if is valid file
	 */
	public static boolean isValidFile(File file) {
		if(file == null || !file.exists() || !file.isFile()) {
			return false;
		}
		return true;
	}

	/**
	 * is directory and directory exists.
	 *
	 * @param file the file
	 * @return true, if successful
	 */
	public static boolean dirExists(File file) {
		return file.exists() && file.isDirectory();
	}

	/**
	 * Dir exists.
	 *
	 * @param fileName the file name
	 * @return true, if successful
	 */
	public static boolean dirExists(String fileName) {
		return dirExists(new File(fileName));
	}

//	public static File getFile(String relativePath) {
//		String path = MapReduceUtils.fullPath(relativePath);
//		return new File(path);
//	}
//
//	/**
//	 * return true if a file or directory exists in file system. It doesn't 
//	 * differentiate directory or file
//	 * @param fileName
//	 * @return
//	 */
//	public static boolean fileOrDirExists(String relativePath) {
//		File file = getFile(relativePath);
//		return file.exists();
//	}
	
	/**
 * return true if the file exists and is a file (not directory).
 *
 * @param file the file
 * @return true, if successful
 */
	public static boolean fileExists(File file) {
		return file != null && file.isFile() && file.exists();
	}

	/**
	 * return true if the given file is a directory, exists, and has files in it .
	 *
	 * @param dir the dir
	 * @return true, if successful
	 */
	public static boolean containsFile(File dir) {
		if(!dirExists(dir)) {
			return false;
		}
		return dir.listFiles().length > 0;
	}

	/**
	 * Check if a folder exists and is a directory, create the folder if
	 * it doesn't exist. 
	 * It doesn't delete any existing 
	 * files in the folders. But it will delete the file with the same 
	 * name as the folder we need to create
	 *
	 * @param folder the folder
	 * @return true if folder exists or was successfully created
	 */
	public static boolean createFolder(File folder) {
		if(folder == null) {
			return false;
		}
		System.out.println("IOUtils.createFolder: " + folder.getAbsolutePath());
		if(folder.exists() && !folder.isDirectory()) { //a file of the same name exists
			System.out.println("Delete folder: " + folder.getAbsolutePath());
			boolean success = folder.delete();
			if(!success) { //can't delete it
				System.err.println("IOUtils.createFolder: can't delete folder");
				return false;
			}
		}
		System.err.println("IOUtils.createFolder: folder exists");
		if(folder.exists() && folder.isDirectory()) {
			return true; //the folder already exists, no need to create new
		}
		System.err.println("IOUtils.createFolder: make folder");
		return folder.mkdirs();
	}

	/**
	 * delete all files in a folder, but not the folder itself.
	 *
	 * @param folder the folder
	 * @return false if argument folder is invalid or some file can't be
	 * deleted.
	 */
	public static boolean clearFolder(File folder) {
		if(folder == null || !folder.exists() || !folder.isDirectory()) {
			return false;
		}
		
		File[] files = folder.listFiles();
		if(files == null) {
			return false; //if not folder, it will be null. For a folder, it will return empty array rather than null
		}
		
		boolean success = true;
		for(File f : files) { //delete each file
			System.out.println("Delete file... " + f.getAbsolutePath());
			success &= f.delete();
		}
		return success;
	}
	
	/**
	 * create a file to write to. It first check if the original file exists, if do, it will
	 * first try to delete the file. So it may delete what you had!
	 * if file is null, false is returned
	 *
	 * @param file the file
	 * @return false for any abnormal
	 */
	public static boolean createFile(File file) {
		if(file == null) {
			return false;
		}
		if(file.exists()) {
			System.out.println("Delete file... " + file.getAbsolutePath());
			file.delete();
		}
		try {
			System.out.println("Create file... " + file.getAbsolutePath());
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * get file extension from file name by searching for the last "." and return the string
	 * after it. abc.def.txt will return "txt". abc will return empty string "". abc/ will return
	 * empty string 
	 *
	 * @param file the file
	 * @return the extension
	 */
	public static String getExtension(File file) {
		if(file.isDirectory()) { // for example, /.dev is a folder, so no extension
			return "";
		}
		String extension = "";
		String fileName = file.getName();
		int i = fileName.lastIndexOf('.');
		if (i > 0) {
		    extension = fileName.substring(i+1);
		}
		return extension;
	}
	
	/**
	 * will rename the file, newName must be full directory.
	 *
	 * @param file the file
	 * @param newName the new name
	 * @return the pointer to the renamed file, or null if fail (like system forbidden)
	 */
	public static File rename(File file, String newName) {
		File newFile = new File(newName);
		boolean rv = file.renameTo(newFile);
		if(!rv) {
			return null;
		}
		return newFile;
	}
	
	/**
	 * add file extension to file name. "/abc" will be renamed to "/abc.txt"
	 * extension must start with ".", like ".txt", must has more than just "."
	 *
	 * @param file the file
	 * @param extension the extension
	 * @return new file,
	 */
	public static File appendExtension(File file, String extension) {
		if(extension.charAt(0) != '.' || extension.length() <= 1) {
			throw new IllegalArgumentException();
		}
		
		String name = file.getAbsolutePath();
		
		if(name.charAt(name.length() - 1) == '/') {
			name = name.substring(0, name.length() - 1);
		}
		name += extension;
		File newFile = rename(file, name);
		if(newFile == null) {
			throw new IllegalArgumentException();
		}
		return newFile;
	}
	
	/**
	 * Delete file.
	 *
	 * @param file the file
	 * @return true, if successful
	 */
	public static boolean deleteFile(File file) {
		if (!fileExists(file)) {
			return true;
		}
		try {
			file.delete();
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	
	/**
	 * set chmod for the given file. File must exists
	 * to set chmod 740, call with user = 7, group = 4, others = 0
	 *
	 * @param file the file
	 * @param user the user
	 * @param group the group
	 * @param others the others
	 * @return true if success
	 */
	public static boolean setFilePermission(File file, int user, int group, int others) {
		if(file == null || !fileExists(file) || 
				user < 0 || group < 0 || others < 0 || 
				user > 7 || group > 7 || others > 7) {
			throw new IllegalArgumentException();
		}
		
		//using PosixFilePermission to set file permissions 777
        Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
        //add owners permission
        if(BinaryUtils.getBit(user, 3) == 1) {        	
        	perms.add(PosixFilePermission.OWNER_READ);
        }
        if(BinaryUtils.getBit(user, 2) == 1) {        	
        	perms.add(PosixFilePermission.OWNER_WRITE);
        }
        if(BinaryUtils.getBit(user, 1) == 1) {
        	perms.add(PosixFilePermission.OWNER_EXECUTE);        	
        }
        //add group permissions
        if(BinaryUtils.getBit(group, 3) == 1) {
        	perms.add(PosixFilePermission.GROUP_READ);
        }
        if(BinaryUtils.getBit(group, 2) == 1) {
        	perms.add(PosixFilePermission.GROUP_WRITE);
        }
        if(BinaryUtils.getBit(group, 1) == 1) {
        	perms.add(PosixFilePermission.GROUP_EXECUTE);
        }
        
        //add others permissions
        if(BinaryUtils.getBit(others, 3) == 1) {
        	perms.add(PosixFilePermission.OTHERS_READ);
        }
        if(BinaryUtils.getBit(others, 2) == 1) {
        	perms.add(PosixFilePermission.OTHERS_WRITE);
        }
        if(BinaryUtils.getBit(others, 1) == 1) {
        	perms.add(PosixFilePermission.OTHERS_EXECUTE);
        }
         
        try {
			Files.setPosixFilePermissions(file.toPath(), perms);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
        return true;
	}
	
	/**
	 * execute a system command, return true if success.
	 *
	 * @param command the command
	 * @return true, if successful
	 */
	public static boolean runtimeExec(String command) {
		Process process = null;
		try {
			process = Runtime.getRuntime().exec(command);
			BufferedReader stdIn 
			= new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line = null;
			while ((line = stdIn.readLine()) != null) {
				System.out.println(line);
			}
			stdIn.close();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
}
