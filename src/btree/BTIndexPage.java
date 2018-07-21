package btree;

import java.io.IOException;

import diskmgr.Page;
import global.PageId;
import global.RID;
import heap.InvalidSlotNumberException;

public class BTIndexPage extends BTSortedPage {
	// Iterator index
	private RID currentRID;

	/**
	 * pin the page with pageno, and get the corresponding BTIndexPage, also it
	 * sets the type of node to be NodeType.INDEX.
	 * 
	 * @param pageno
	 * @param keyType
	 * @throws ConstructPageException
	 * @throws IOException
	 */
	public BTIndexPage(PageId pageno, int keyType)
			throws ConstructPageException, IOException {
		super(pageno, keyType);
		setType(NodeType.INDEX);
	}

	/**
	 * associate the BTIndexPage instance with the Page instance, also it sets
	 * the type of node to be NodeType.INDEX.
	 * 
	 * @param page
	 * @param keyType
	 * @throws IOException
	 */
	public BTIndexPage(Page page, int keyType) throws IOException {
		super(page, keyType);
		setType(NodeType.INDEX);
	}

	public BTIndexPage(int keyType) throws ConstructPageException, IOException {
		super(keyType);
		setType(NodeType.INDEX);
	}

	/**
	 * It inserts a value into the index page,
	 * 
	 * @param key
	 * @param pageNo
	 * @return
	 * @throws InsertRecException
	 */
	public RID insertKey(KeyClass key, PageId pageNo) throws InsertRecException {
		return super.insertRecord(new KeyDataEntry(key, pageNo));
	}

	/**
	 * This function encapsulates the search routine to search a BTIndexPage by
	 * B++ search algorithm
	 * 
	 * @param key
	 *            the key value used in search algorithm. Input parameter.
	 * @return It returns the page_no of the child to be searched next.
	 * @exception IndexSearchException
	 *                Index search failed;
	 */

	public PageId getPageNoByKey(KeyClass key)
			throws InvalidSlotNumberException, IOException,
			KeyNotMatchException, NodeNotMatchException, ConvertException {
		KeyDataEntry entry;
		PageId lastPid = getLeftLink();
		byte[] byteArr;
		
		RID iter = firstRecord();
		if (iter == null)
			return getPrevPage();
		while (true) {
			byteArr = getRecord(iter).getTupleByteArray();
			entry = BT.getEntryFromBytes(byteArr, 0, byteArr.length, keyType, getType());
			if (nextRecord(iter) == null)
				return ((IndexData) entry.data).getData();
			if(BT.keyCompare(key, entry.key) < 0)
				return lastPid;
			lastPid = ((IndexData) entry.data).getData();
			iter = nextRecord(iter);
		}
	}

	/**
	 * Iterators. One of the two functions: getFirst and getNext which provide
	 * an iterator interface to the records on a BTIndexPage.
	 * 
	 * @param rid
	 * @return
	 * @throws IOException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 * @throws InvalidSlotNumberException
	 */
	public KeyDataEntry getFirst(RID rid) throws IOException,
			KeyNotMatchException, NodeNotMatchException, ConvertException,
			InvalidSlotNumberException {
		RID nrid = firstRecord();
		rid.pageNo = nrid.pageNo;
		rid.slotNo = nrid.slotNo;
		if (currentRID == null) {
			currentRID = nrid;
		}
		byte[] byteArr = getRecord(nrid).getTupleByteArray();
		return BT.getEntryFromBytes(byteArr, 0, byteArr.length, keyType,
				getType());
	}

	/**
	 * Iterators. One of the two functions: getFirst and getNext which provide
	 * an iterator interface to the records on a BTIndexPage.
	 * 
	 * @param rid
	 * @return
	 * @throws IOException
	 * @throws InvalidSlotNumberException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 */
	public KeyDataEntry getNext(RID rid) throws InvalidSlotNumberException,
			IOException, KeyNotMatchException, NodeNotMatchException,
			ConvertException {
		currentRID = nextRecord(currentRID);
		if (currentRID != null) {
			rid.pageNo = currentRID.pageNo;
			rid.slotNo = currentRID.slotNo;
			byte[] byteArr = getRecord(currentRID).getTupleByteArray();
			return BT.getEntryFromBytes(byteArr, 0, byteArr.length, keyType,
					getType());
		} else {
			return null;
		}
	}

	/**
	 * Left Link You will recall that the index pages have a left-most pointer
	 * that is followed whenever the search key value is less than the least key
	 * value in the index node. The previous page pointer is used to implement
	 * the left link.
	 * 
	 * @return
	 * @throws IOException
	 */
	public PageId getLeftLink() throws IOException {
		return getPrevPage();
	}

	public void setLeftLink(PageId left) throws IOException {
		setPrevPage(left);
	}
}
