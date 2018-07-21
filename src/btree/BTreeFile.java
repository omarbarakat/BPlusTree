package btree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Stack;
import bufmgr.BufMgr;
import diskmgr.DB;
import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;

public class BTreeFile extends IndexFile {

	private String fileName;
	private BufMgr bufMgr;
	private DB db;
	private PageId headerID;
	private BTreeHeaderPage header;

	/**
	 * If index file exists, open it; else create it.
	 * 
	 * @param fName
	 * @param keytype
	 * @param keysize
	 * @param delete_fashion
	 */
	public BTreeFile(String fName, int keytype, int keysize, int delete_fashion) {
		if (fName != null) {
			fileName = fName;
		} else {
			// TROUBLE ?
		}
		bufMgr = SystemDefs.JavabaseBM;
		db = SystemDefs.JavabaseDB;

		headerID = null;
		try {
			headerID = db.get_file_entry(fileName);
			if (headerID == null) {
				header = new BTreeHeaderPage();
				headerID = bufMgr.newPage(header, 1);
				header.initHeader(headerID);

				header.setSearchKeyType(keytype);
				header.setMaxKeyLength(keysize);
				header.setDeleteFashion(delete_fashion);
				header.setType(NodeType.BTHEAD);

				BTSortedPage root = new BTSortedPage(keytype);
				header.setRootID(root.getCurPage());
				bufMgr.unpinPage(root.getCurPage(), true);

				db.add_file_entry(fileName, headerID);
				// bufMgr.unpinPage(headerID, true); // should stay pinned
			} else {
				header = new BTreeHeaderPage();
				bufMgr.pinPage(headerID, header, false);
				header.readHPageIn();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * BTreeFile class an index file with given filename should already exist;
	 * this opens it.
	 * 
	 * @param filename
	 */
	public BTreeFile(String filename) {
		this(filename, 0, 0, 0);
	}

	/**
	 * Insert record with the given key and rid.
	 */
	static int i = 0;

	public void insert(KeyClass key, RID rid) {
		try {
			i++;
			Stack<PageId> path;

			path = getPathForSpecificKey(key); // get path to the key
			BTSortedPage node = new BTSortedPage(path.peek(),
					header.getSearchKeyType());
			// System.out.println(path + " -------- pid to insert in " +
			// path.peek() + " with slot_cnt = "+node.getSlotCnt());

			KeyDataEntry newEntry = new KeyDataEntry(key, rid);
			if (node.insertRecord(newEntry) != null) { // inserted successfully
				bufMgr.unpinPage(node.getCurPage(), true);
				return;
			}
			System.out.println(i + " ndCnt = " + node.getSlotCnt());
			// ----------- split leaf node -----------\\
			byte[] tempFash5 = new byte[node.getpage().length];
			System.arraycopy(node.getpage(), 0, tempFash5, 0, tempFash5.length);
			BTLeafPage newLeafPage = splitLeafNode(node);

			// --------- add the new entery ----------\\
			byte[] tupleContent = newLeafPage.getRecord(
					newLeafPage.firstRecord()).getTupleByteArray();
			KeyDataEntry entry = BT.getEntryFromBytes(tupleContent, 0,
					tupleContent.length, header.getSearchKeyType(),
					NodeType.LEAF);

			if (BT.keyCompare(key, entry.key) >= 0) {
				newLeafPage.insertRecord(newEntry);
			} else {
				node.insertRecord(newEntry);
			}
			// ---------------------------------------\\

			// ---- make entry with the first key in newLeafPage and the
			// newLeafPageId, and assign it to be copied up
			// ---- this is saved in the newEntry variable (entry is just temp)
			newEntry = new KeyDataEntry(entry.key, newLeafPage.getCurPage());

			// ---- add the index entry in the tree. full path is sent in case
			// further splits were needed
			bufMgr.unpinPage(path.pop(), true); // remove the leaf page from the
												// path to the index page
			addIndexEntryAt(newEntry, path);
			bufMgr.unpinPage(newLeafPage.getCurPage(), true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Stack<PageId> getPathForSpecificKey(KeyClass key) {
		try {
			BTSortedPage node = new BTSortedPage(new Page(),
					header.getSearchKeyType());
			BTIndexPage temp = new BTIndexPage(new Page(),
					header.getSearchKeyType());

			Stack<PageId> path = new Stack<PageId>();
			path.push(header.getRootID());
			while (true) {

				bufMgr.pinPage(path.peek(), node, false);

				if (node.getType() == NodeType.INDEX) {
					temp.setpage(node.getpage());
					bufMgr.unpinPage(node.getCurPage(), false);
					path.push(temp.getPageNoByKey(key));
				} else {
					break; // now the last pid in the stack is the leafPage to
							// add the rec to
				}
			}
			return path;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private void addIndexEntryAt(KeyDataEntry newEnt, Stack<PageId> pathToRoot) {
		try {
			byte[] tupleContent;
			KeyDataEntry temp = null, newEntry = new KeyDataEntry(newEnt.key,
					newEnt.data);
			BTSortedPage node = new BTSortedPage(new Page(),
					header.getSearchKeyType());
			BTIndexPage newIndexPage;

			while (!pathToRoot.isEmpty()) {
				bufMgr.pinPage(pathToRoot.pop(), node, false);

				if (node.available_space() >= BT.getKeyDataLength(newEntry.key,
						NodeType.INDEX)) {
					node.insertRecord(newEntry);
					bufMgr.unpinPage(node.getCurPage(), true);
					return; // insertion succeed
				}

				// need to split index node
				newIndexPage = new BTIndexPage(header.getSearchKeyType());

				// move (the second) half of records to newIndexNode
				RID movId = new RID(node.getCurPage(), node.getSlotCnt() - 1); // RID
																				// of
																				// record
																				// to
																				// be
																				// moved
				while (node.available_space() < 0.5 * BTSortedPage.MINIBASE_PAGESIZE) {
					movId.slotNo = node.getSlotCnt() - 1;
					tupleContent = node.getRecord(movId).getTupleByteArray();
					temp = BT.getEntryFromBytes(tupleContent, 0,
							tupleContent.length, header.getSearchKeyType(),
							NodeType.INDEX);
					newIndexPage.insertRecord(temp);
					node.deleteSortedRecord(movId);
				}

				// ---- make newEntry with the first key in newIndexPage and the
				// newIndexPageId, and assign it to be pushed up
				// ---- assign value to the left branch of the newIndexPage
				tupleContent = newIndexPage.getRecord(
						newIndexPage.firstRecord()).getTupleByteArray();
				temp = BT.getEntryFromBytes(tupleContent, 0,
						tupleContent.length, header.getSearchKeyType(),
						NodeType.INDEX);
				newEntry = new KeyDataEntry(temp.key, newIndexPage.getCurPage());
				newIndexPage.deleteSortedRecord(newIndexPage.firstRecord());
				newIndexPage.setLeftLink(((IndexData) temp.data).getData());

				bufMgr.unpinPage(node.getCurPage(), true);
				bufMgr.unpinPage(newIndexPage.getCurPage(), true);
			}
			// if it comes here, a new root is needed
			BTIndexPage newRoot = new BTIndexPage(header.getSearchKeyType());
			newRoot.insertRecord(newEntry);
			newRoot.setLeftLink(header.getRootID());
			header.setRootID(newRoot.getCurPage());
			bufMgr.unpinPage(newRoot.getCurPage(), true);

		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	/**
	 * leaves the new page (if needed) pinned
	 */

	private BTLeafPage splitLeafNode(BTSortedPage node) {

		try {
			BTLeafPage newLeafPage = new BTLeafPage(header.getSearchKeyType());
			PageId newLeafPageId = newLeafPage.getCurPage();

			newLeafPage.setNextPage(node.getNextPage());
			newLeafPage.setPrevPage(node.getCurPage());
			node.setNextPage(newLeafPageId);

			// move (the second) half of records to newLeafNode
			byte[] tupleContent;
			KeyDataEntry entry = null;
			RID movId = new RID(node.getCurPage(), node.getSlotCnt() - 1);

			// System.out.println(i + "  before splitting : slot count in node "
			// + node.getSlotCnt());
			int xx = 0;
			while (node.available_space() < 0.5 * BTSortedPage.MINIBASE_PAGESIZE) {
				System.out.println("xx of " + i + " = " + xx++);
				movId.slotNo = node.getSlotCnt() - 1;
				// if (node.getSlotCnt() == 0) {System.out.println(i +
				// "*****END"); System.exit(0);}
				tupleContent = node.getRecord(movId).getTupleByteArray();
				entry = BT.getEntryFromBytes(tupleContent, 0,
						tupleContent.length, header.getSearchKeyType(),
						NodeType.LEAF);
				newLeafPage.insertRecord(entry);
				node.deleteSortedRecord(movId);
			}

			return newLeafPage;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * delete leaf entry given its pair. `rid' is IN the data entry; it is not
	 * the id of the data entry)
	 */
	static int j = 0;

	public boolean Delete(KeyClass data, RID rid) {
		try {
			BTSortedPage iter = new BTIndexPage(header.getRootID(),
					header.getSearchKeyType());
			while (true) {

				if (iter.getType() == NodeType.LEAF) {
					System.out.println("leaf");
					System.out.println(j++);
					bufMgr.unpinPage(iter.getCurPage(), false);
					iter = new BTLeafPage(iter.getCurPage(),
							header.getSearchKeyType());
					boolean b = ((BTLeafPage) iter).delEntry(new KeyDataEntry(
							data, rid));
					bufMgr.unpinPage(iter.getCurPage(), false);
					return b;
				}
				PageId next = ((BTIndexPage) iter).getPageNoByKey(data);
				bufMgr.unpinPage(iter.getCurPage(), false);

				iter = new BTSortedPage(next, header.getSearchKeyType());

				iter = new BTIndexPage(next, header.getSearchKeyType());
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Close the B+ tree file. Unpin header page.
	 */
	public void close() {
		try {
			bufMgr.unpinPage(headerID, false);
			bufMgr = null;
			db = null;
			headerID = null;
			header = null;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void Rec_Delete_Page(ArrayList<PageId> x) {
		if (x.isEmpty())
			return;

		ArrayList<ArrayList<PageId>> xx = new ArrayList<ArrayList<PageId>>(
				x.size());
		for (int i = 0; i < x.size(); i++) {
			xx.add(new ArrayList<PageId>());
		}
		int i = 0;
		for (PageId p : x) {
			xx.get(i).addAll(getChildren(p));
			try {
				db.deallocate_page(p);
			} catch (Exception e) {
				e.printStackTrace();
			}
			i++;
		}
		for (ArrayList<PageId> arrayList : xx) {
			Rec_Delete_Page(arrayList);
		}
	}

	private ArrayList<PageId> getChildren(PageId p) {
		try {
			ArrayList<PageId> toRet = new ArrayList<PageId>();
			BTSortedPage page = new BTSortedPage(p, header.getSearchKeyType());
			if (page.getType() == NodeType.LEAF)
				return toRet;

			toRet.add(page.getPrevPage());
			int i = 0;
			while (i < page.getSlotCnt()) {
				KeyDataEntry e = BT.getEntryFromBytes(page.getpage(),
						page.getSlotOffset(i), page.getSlotLength(i),
						header.getSearchKeyType(), NodeType.INDEX);
				toRet.add(((IndexData) e.data).getData());
				i++;
			}
			bufMgr.unpinPage(p, false);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Destroy entire B+ tree file.
	 */
	public void destroyFile() {
		try {
			ArrayList<PageId> root = new ArrayList<PageId>();
			root.add(header.getRootID());
			Rec_Delete_Page(root);
			db.deallocate_page(headerID);
			db.delete_file_entry(fileName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * create a scan with given keys Cases:
	 * 
	 * (1) lo_key = null, hi_key = null --> scan the whole index
	 * 
	 * (2) lo_key = null, hi_key!= null --> range scan from min to the hi_key
	 * 
	 * (3) lo_key!= null, hi_key = null --> range scan from the lo_key to max
	 * 
	 * (4) lo_key!= null, hi_key!= null, lo_key = hi_key --> exact match ( might
	 * not unique)
	 * 
	 * (5) lo_key!= null, hi_key!= null, lo_key < hi_key --> range scan from
	 * lo_key to hi_key
	 * 
	 * @param lo_key
	 * @param hi_key
	 * @return
	 */
	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key) {
		try {
			return new BTFileScan(lo_key, hi_key, header);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public BTreeHeaderPage getHeaderPage() throws IOException {
		return header;
	}

	// NEGLECTED
	public void traceFilename(String string) {

	}
}
