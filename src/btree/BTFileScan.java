package btree;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.InvalidSlotNumberException;

/**
 * To do this, we need an initial RID to start the scan with, and we need a
 * final key value to know where to stop.
 * 
 * @author Omar
 */
public class BTFileScan extends IndexFileScan {

	private KeyClass hiKey;
	private RID loRID;

	private BTLeafPage currPage;
	private RID currRID;

	private boolean done;

	/**
	 * Header Page, for information retrieval
	 */
	private BTreeHeaderPage header;

	/**
	 * 
	 * (1) lo_key = null, hi_key = null --> scan the whole index
	 * 
	 * (2) lo_key = null, hi_key!= null --> range scan from min to the hi_key
	 * 
	 * (3) lo_key!= null, hi_key = null --> range scan from the lo_key to max
	 * 
	 * (4) lo_key!= null, hi_key!= null, lo_key = hi_key --> exact match ( might
	 * not be unique)
	 * 
	 * (5) lo_key!= null, hi_key!= null, lo_key < hi_key --> range scan from
	 * lo_key to hi_key
	 * 
	 * @param lo_key
	 * @param hi_key
	 * @return
	 * @throws KeyNotMatchException
	 */
	public BTFileScan(KeyClass lo_key, KeyClass hi_key, BTreeHeaderPage header)
			throws ConstructPageException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, ReplacerException,
			PageUnpinnedException, HashEntryNotFoundException,
			InvalidFrameNumberException, IOException,
			InvalidSlotNumberException, HashOperationException,
			PageNotReadException, BufferPoolExceededException,
			PagePinnedException, BufMgrException {

		this.header = header;
		done = false;

		if (lo_key == null && hi_key == null) {
			// scan the whole index
			loRID = getMinimumRID();
			hiKey = getMaxKey();
		} else if (lo_key == null && hi_key != null) {
			// range scan from min to the hi_key
			loRID = getMinimumRID();
			hiKey = hi_key;
		} else if (lo_key != null && hi_key == null) {
			// range scan from the lo_key to max
			loRID = getFromKey(lo_key);
			hiKey = getMaxKey();
		} else if (lo_key != null && hi_key != null) {
			if (BT.keyCompare(lo_key, hi_key) <= 0) {
				// exact match (might not be unique)
				// or
				// range scan from lo_key to hi_key
				loRID = getFromKey(lo_key);
				hiKey = hi_key;
			} else {
				// Assumption
				loRID = getFromKey(hi_key);
				hiKey = lo_key;
			}
		}
		new BTSortedPage(5);
		currPage = new BTLeafPage(loRID.pageNo, header.getSearchKeyType());
		currRID = loRID;
	}

	private RID getFromKey(KeyClass k) throws ConstructPageException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, ReplacerException,
			PageUnpinnedException, HashEntryNotFoundException,
			InvalidFrameNumberException, HashOperationException,
			PageNotReadException, BufferPoolExceededException,
			PagePinnedException, BufMgrException {

		PageId rootId = header.getRootID();
		BTSortedPage iter = new BTSortedPage(rootId, header.getSearchKeyType());
		while (true) {
			if (iter.getType() == NodeType.INDEX) {
				PageId temp = ((BTIndexPage) iter).getPageNoByKey(k);
				SystemDefs.JavabaseBM.unpinPage(iter.getCurPage(), false);
				iter = new BTSortedPage(temp, header.getSearchKeyType());
			} else if (iter.getType() == NodeType.LEAF) {
				KeyDataEntry entry;
				for (int i = 0; i < iter.getSlotCnt(); i++) {
					entry = BT.getEntryFromBytes(iter.getpage(),
							iter.getSlotOffset(i), iter.getSlotLength(i),
							header.getSearchKeyType(), iter.getType());
					if (BT.keyCompare(entry.key, k) == 0) {
						RID temp = new RID(iter.getCurPage(), i);
						SystemDefs.JavabaseBM.unpinPage(iter.getCurPage(),
								false);
						return temp;
					}
				}

			}
		}
	}

	private KeyClass getMaxKey() throws ConstructPageException, IOException,
			KeyNotMatchException, NodeNotMatchException, ConvertException,
			ReplacerException, PageUnpinnedException,
			HashEntryNotFoundException, InvalidFrameNumberException {
		PageId rootId = header.getRootID();
		BTSortedPage iter = new BTSortedPage(rootId, header.getSearchKeyType());
		KeyDataEntry entry;
		while (true) {
			int lastPos = iter.getSlotCnt() - 1;
			entry = BT.getEntryFromBytes(iter.getpage(),
					iter.getSlotOffset(lastPos), iter.getSlotLength(lastPos),
					header.getSearchKeyType(), iter.getType());
			SystemDefs.JavabaseBM.unpinPage(iter.getCurPage(), false);
			if (iter.getType() == NodeType.INDEX) {
				iter = new BTSortedPage(((IndexData) entry.data).getData(),
						header.getSearchKeyType());
			} else if (iter.getType() == NodeType.LEAF) {
				return entry.key;
			}
		}
	}

	private RID getMinimumRID() throws ConstructPageException, IOException,
			KeyNotMatchException, NodeNotMatchException, ConvertException,
			ReplacerException, PageUnpinnedException,
			HashEntryNotFoundException, InvalidFrameNumberException {
		PageId rootId = header.getRootID();
		BTIndexPage iter = new BTIndexPage(rootId, header.getSearchKeyType());
		while (true) {
			if (iter.getType() == NodeType.LEAF) {
				KeyDataEntry entry = BT.getEntryFromBytes(iter.getpage(),
						iter.getSlotOffset(0), iter.getSlotLength(0),
						header.getSearchKeyType(), iter.getType());
				SystemDefs.JavabaseBM.unpinPage(iter.getCurPage(), false);
				return ((LeafData) entry.data).getData();
			}
			
			PageId  x = iter.getPrevPage();
			SystemDefs.JavabaseBM.unpinPage(iter.getCurPage(), false);
			iter = new BTIndexPage(((BTIndexPage) iter).getLeftLink(),
					header.getSearchKeyType());
		}

	}

	/**
	 * Iterate once (during a scan).
	 * 
	 * Returns: null if done; otherwise next KeyDataEntry
	 */
	@Override
	public KeyDataEntry get_next() {
		try {
			if (!done) {
				KeyDataEntry toRet = BT.getEntryFromBytes(currPage.getpage(),
						currPage.getSlotOffset(currRID.slotNo),
						currPage.getSlotLength(currRID.slotNo),
						currPage.keyType, NodeType.LEAF);
				updateCurrent();
				return toRet;
			}
			return null;
		} catch (Exception e) {
			return null;
		}
	}

	private void updateCurrent() {
		try {
			RID currCand = currPage.nextRecord(currRID);
			if (currCand != null) {
				KeyClass cur = BT.getEntryFromBytes(currPage.getpage(),
						currPage.getSlotOffset(currCand.slotNo),
						currPage.getSlotLength(currCand.slotNo),
						currPage.keyType, NodeType.LEAF).key;
				if (BT.keyCompare(cur, hiKey) <= 0) {
					currRID = currCand;
				} else {
					done = true;
				}
			} else {
				PageId nextPage = currPage.getNextPage();
				if (nextPage.pid != -1) {
					// Not the last page in the list
					SystemDefs.JavabaseBM.unpinPage(currRID.pageNo, false);
					SystemDefs.JavabaseBM.pinPage(nextPage, currPage, false);

					currCand = currPage.firstRecord();

					KeyClass cur = BT.getEntryFromBytes(currPage.getpage(),
							currPage.getSlotOffset(currCand.slotNo),
							currPage.getSlotLength(currCand.slotNo),
							currPage.keyType, NodeType.LEAF).key;
					if (BT.keyCompare(cur, hiKey) <= 0) {
						currRID = currCand;
					} else {
						done = true;
					}
				} else {
					// The last page in the list
					done = true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Delete currently-being-scanned(i.e., just scanned) data entry.
	 */
	@Override
	public void delete_current() {
		try {
			currPage.deleteSortedRecord(currRID);
		} catch (Exception e) {

		}
	}

	/**
	 * max size of the key
	 */
	@Override
	public int keysize() {
		return header.getMaxKeyLength();
	}

	/**
	 * destructor. unpin some pages if they are not unpinned already. and do
	 * some clearing work.
	 */
	public void DestroyBTreeFileScan() {

	}

}
