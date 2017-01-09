package com.ms.silverking.cloud.skfs.dir;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import com.google.common.io.Files;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.text.StringUtil;

public class Copy2SKFS implements FileFilter {
	private final Mode	mode;
	private final SKGridConfiguration	gc;
	private final String	skfsBase;
	
	private enum Mode {NonRecursive, Recursive};
	
	private static final boolean	debug = false;
	
	private static final String	skfsRelativeBase = "/skfs"; 
	
	public Copy2SKFS(SKGridConfiguration gc, String skfsBase, Mode mode) {
		this.gc = gc;
		this.skfsBase = skfsBase;
		this.mode = mode;
	}
	
	@Override
	public boolean accept(File file) {
		if (mode == Mode.Recursive) {
			return true;
		} else {
			return file.isFile();
		}
	}
	
	private File fullPathSKFS(File relativePath) {
		return fullPathSKFS(relativePath.toString());
	}
	
	private File fullPathSKFS(String relativePath) {
		return new File(skfsBase +"/"+ relativePath.toString());
	}
	
	public void copy(File src, String dst) throws ClientException, IOException {
		File[]	files;
		
		if (debug) {
			System.out.printf("in copy %s %s\n", src.toString(), dst);
		}
		if (mode == Mode.NonRecursive && src.isDirectory()) {
			if (debug) {
				System.out.printf("NonRecursive. Ignoring directory: %s\n", src.toString());
			}
		} else {
			boolean	dstIsDir;
			
			if (src.isDirectory()) {
				files = src.listFiles(this);
				if (debug) {
					System.out.printf("Create dir %s\n", dst +"/"+ src.getName());
				}
				new SimpleDirCreator(gc).create(SimpleDir.createFrom(files), dst +"/"+ src.getName());
				dstIsDir = true;
			} else {
				files = new File[1];
				files[0] = src;
				dstIsDir = false;
			}
			for (File srcFile : files) {
				File	dstFile;
				
				if (srcFile.isFile()) {
					if (dstIsDir) {
						dstFile = new File(dst +"/"+ src.getName() +"/"+ srcFile.getName());
					} else {
						dstFile = new File(dst +"/"+ src.getName());
					}
					if (dstFile.isDirectory())
					if (debug) {
						System.out.printf("copying %s %s\n", srcFile.toString(), dstFile.toString());
					}
					Files.copy(srcFile, fullPathSKFS(dstFile));
				} else if (srcFile.isDirectory()) {
					dstFile = new File(dst +"/"+ src.getName());
					copy(srcFile, dstFile.getAbsolutePath());
				}
			}
		}
		if (debug) {
			System.out.printf("out copy %s %s\n", src.toString(), dst);
		}
	}

	public void cp2skfs(File[] srcFiles, String dst) throws ClientException, IOException {
		if (!dst.equals(skfsRelativeBase)) {
			File	dstFile;
			
			dstFile = new File(dst);
			if (srcFiles.length == 1 && srcFiles[0].isDirectory()) {
				if (false && fullPathSKFS(dstFile).exists()) {
					System.err.printf("Can't create %s. File exists.\n", dstFile.toString());
					System.exit(-1);
				} else {
					if (debug) {
						System.out.printf("Creating dest dir %s\n", dstFile.toString());
					}
					srcFiles = srcFiles[0].listFiles(this);
					new SimpleDirCreator(gc).create(SimpleDir.createFrom(srcFiles), dstFile.toString());
				}
			} else {
				if (debug) {
					System.out.printf("No special case dest %s %s %s\n", 
							(srcFiles.length == 1), srcFiles[0], srcFiles[0].isDirectory());
				}
			}
		}
		for (File srcFile : srcFiles) {
			copy(srcFile, dst);
		}
	}
	
	private static boolean isValidDest(String dst) {
		if (dst.equals(skfsRelativeBase)) {
			return true;
		} else {
			if (!dst.startsWith(skfsRelativeBase)) {
				return false;
			} else {
				String	tail;
				
				tail = dst.substring(skfsRelativeBase.length());
				if (!tail.startsWith("/") || StringUtil.countOccurrences(dst, '/') != 1) {
					return false;
				} else {
					return true;
				}
			}
		}
	}
	
	public static void main(String[] args) {
		Mode	mode;
		int		argBaseIndex;
		int		minArgs;
		String		skfsBase;

		if (args.length > 1) {
			skfsBase = args[0];
		} else {
			skfsBase = null;
		}
		if (args.length > 2 && args[1].equals("-r")) {
			mode = Mode.Recursive;
			minArgs = 5;
			argBaseIndex = 2;
		} else {
			mode = Mode.NonRecursive;
			minArgs = 4;
			argBaseIndex = 1;
		}
		
		if (args.length < minArgs) {
			System.err.println("args: <skfsBase> [-r] <gridConfig> <src...> <dst>");
			System.exit(-1);
		} else {
			try {
				Copy2SKFS	cp2skfs;
				SKGridConfiguration	gc;
				String		dst;
				File[]		src;
				
				gc = SKGridConfiguration.parseFile(args[argBaseIndex + 0]);
				cp2skfs = new Copy2SKFS(gc, skfsBase, mode);
				dst = args[args.length - 1];
				if (dst.startsWith(skfsBase)) {
					dst = dst.substring(skfsBase.length());
				}
				if (debug) {
					System.out.printf("Using dst: %s\n", dst);
				}
				if (isValidDest(dst)) {
					System.err.printf("Currently, destination must be %s/%s, %s,\n"
							+"or an immediate subdir for a single directory source and recursive copy\n", 
							skfsBase, skfsRelativeBase, skfsRelativeBase, dst, dst);
					System.exit(-1);
				}
				src = new File[(args.length - 1) - (argBaseIndex + 1)];
				for (int i = argBaseIndex + 1; i < args.length - 1; i++) {
					src[i - (argBaseIndex + 1)] = new File(args[i]);
				}
				cp2skfs.cp2skfs(src, dst);
				System.exit(0);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
	}
}
