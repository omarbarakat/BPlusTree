package btree;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import global.Convert;
import global.PageId;
import global.RID;
import heap.HFPage;
import heap.Tuple;

/**
 * header page is used to hold information about the tree as a whole, such as
 * the page id of the root page, the type of the search key, the length of the
 * key field(s) (which has a fixed maximum size in this assignment), etc.
 */

public class BTreeHeaderPage extends HFPage {

	private Queue<RID> container;
	private static final int qSize = 4;

	/*
	 * Order within the queue (and within the data array):
	 */
	private static final int rootID = 0;
	private static final int SKType = 1;
	private static final int maxKLen = 2;
	private static final int delFashion = 3;

	public BTreeHeaderPage() throws IOException {
		super();
		setType(NodeType.BTHEAD);
	}

	public void initHeader(PageId pid) {
		try {
			init(pid, this);
			container = new LinkedList<RID>();
			byte[] f = new byte[Integer.SIZE / 8];
			Convert.setIntValue(-1, 0, f);
			for (int i = 0; i < qSize; i++)
				container.offer(insertRecord(f));
		} catch (Exception e) {

		}
	}

	public void readHPageIn() {
		try {
			available_space();
			container = new LinkedList<RID>();
			RID temp = firstRecord();
			for (int i = 0; i < qSize; i++) {
				container.offer(temp);
				temp = nextRecord(temp);
			}
		} catch (Exception e) {

		}
	}

	// /////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////

	private void setInfo(int pos, int nValue) {
		try {
			byte[] f = new byte[Integer.SIZE / 8];
			for (int i = 0; i < qSize; i++) {
				RID top = container.poll();
				int val = Convert.getIntValue(0, getRecord(top)
						.getTupleByteArray());
				deleteRecord(top);
				if (i == pos) {
					Convert.setIntValue(nValue, 0, f);
					container.offer(insertRecord(f));
				} else {
					Convert.setIntValue(val, 0, f);
					container.offer(insertRecord(f));
				}
			}
		} catch (Exception e) {

		}
	}

	// /////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////

	public void setRootID(PageId pid) {
		setInfo(rootID, pid.pid);
	}

	public void setSearchKeyType(int value) {
		setInfo(SKType, value);
	}

	public void setMaxKeyLength(int value) {
		setInfo(maxKLen, value);
	}

	public void setDeleteFashion(int value) {
		setInfo(delFashion, value);
	}

	// /////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////

	public PageId getRootID() {
		return getPage(rootID);
	}
	
	public PageId get_rootId() {
		return getPage(rootID);
	}

	public int getSearchKeyType() {
		return getInteger(SKType);
	}
	
	public short get_keyType(){
		return (short)getSearchKeyType();
	}

	public int getMaxKeyLength() {
		return getInteger(maxKLen);
	}

	public int getDelFashion() {
		return getInteger(delFashion);
	}

	// /////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////

	private int getInteger(int pos) {
		try {
			LinkedList<RID> temp = (LinkedList<RID>) container;
			Tuple t = getRecord(temp.get(pos));
			int x = Convert.getIntValue(0, t.getTupleByteArray());
			return x;
		} catch (Exception e) {
			return 0;
		}
	}

	private PageId getPage(int pos) {
		try {
			LinkedList<RID> temp = (LinkedList<RID>) container;

			PageId pid = new PageId(Convert.getIntValue(0,
					getRecord(temp.get(pos)).getTupleByteArray()));
			if (pid.pid == -1)
				return null;
			return pid;
		} catch (Exception e) {

			return null;
		}
	}

}
