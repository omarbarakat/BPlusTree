package btree;

import java.io.IOException;

import diskmgr.Page;
import global.PageId;
import global.RID;
import heap.InvalidSlotNumberException;

public class BTLeafPage extends BTSortedPage {
	// Iterator index
	private RID currentRID;

	/**
	 * pin the page with pageno, and get the corresponding BTLeafPage, also it
	 * sets the type to be NodeType.LEAF.
	 * 
	 * @param pageno
	 * @param keyType
	 * @throws IOException
	 */
	public BTLeafPage(PageId pageno, int keyType)
			throws ConstructPageException, IOException {
		super(pageno, keyType);
		setType(NodeType.LEAF);
	}

	/**
	 * associate the BTLeafPage instance with the Page instance, also it sets
	 * the type to be NodeType.LEAF.
	 * 
	 * @param page
	 * @param keyType
	 * @throws IOException
	 */
	public BTLeafPage(Page page, int keyType) throws IOException {
		super(page, keyType);
		setType(NodeType.LEAF);
	}

	/**
	 * new a page, associate the BTLeafPage instance with the Page instance,
	 * also it sets the type to be NodeType.LEAF.
	 * 
	 * @param keyType
	 * @throws ConstructPageException
	 * @throws IOException
	 */
	public BTLeafPage(int keyType) throws ConstructPageException, IOException {
		super(keyType);
		setType(NodeType.LEAF);
	}

	/**
	 * insertRecord. READ THIS DESCRIPTION CAREFULLY. THERE ARE TWO RIDs WHICH
	 * MEAN TWO DIFFERENT THINGS. Inserts a key, rid value into the leaf node.
	 * This is accomplished by a call to SortedPage::insertRecord()
	 * 
	 * @param key
	 * @param dataRid
	 * @return
	 * @throws InsertRecException
	 */
	public RID insertRecord(KeyClass key, RID dataRid)
			throws InsertRecException {
		return super.insertRecord(new KeyDataEntry(key, dataRid));
	}

	/**
	 * Iterators. One of the two functions: getFirst and getNext which provide
	 * an iterator interface to the records on a BTLeafPage.
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
	 * an iterator interface to the records on a BTLeafPage.
	 * 
	 * @param rid
	 * @return
	 * @throws IOException
	 * @throws InvalidSlotNumberException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 */
	public KeyDataEntry getNext(RID rid) throws IOException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException {
		currentRID = nextRecord(currentRID);
		if (currentRID != null) {
			return getCurrent(rid);
		} else {
			return null;
		}
	}

	/**
	 * getCurrent returns the current record in the iteration; it is like
	 * getNext except it does not advance the iterator.
	 * 
	 * @param rid
	 * @return
	 * @throws IOException
	 * @throws InvalidSlotNumberException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 */
	public KeyDataEntry getCurrent(RID rid) throws InvalidSlotNumberException,
			IOException, KeyNotMatchException, NodeNotMatchException,
			ConvertException {
		rid.pageNo = currentRID.pageNo;
		rid.slotNo = currentRID.slotNo;
		byte[] byteArr = getRecord(currentRID).getTupleByteArray();
		return BT.getEntryFromBytes(byteArr, 0, byteArr.length, keyType,
				getType());
	}

	/**
	 * delete a data entry in the leaf page.
	 * 
	 * @param dEntry
	 *            the entry will be deleted in the leaf page. Input parameter.
	 * @return true if deleted; false if no dEntry in the page
	 * @throws DeleteRecException
	 * @throws IOException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 * @throws InvalidSlotNumberException
	 * @exception LeafDeleteException
	 *                error when delete
	 */
	public boolean delEntry(KeyDataEntry dEntry) throws DeleteRecException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, IOException {
		RID tempRID = new RID();
		KeyDataEntry entry = getFirst(tempRID);
		while (entry != null) {
			if (entry.equals(dEntry)) {
				return super.deleteSortedRecord(tempRID);
			}
			entry = getNext(tempRID);
		}
		return false;
	}
}
