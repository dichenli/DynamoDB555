package SearchUtils;

import java.util.List;

public class FindURLThread extends Thread{
	
	int index;
	List<DocResult> set;
	int size;
	
	public FindURLThread(int index, List<DocResult> set, int size){
		this.index = index;
		this.set = set;
		this.size = size;
	}

	@Override
	public void run() {
		for(int i=0;i<size;i++){
			if(i%10 == index){
				DocResult doc = set.get(i);
				try {
					doc.analyzeURLTitle();
					doc.secondScore();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

}
