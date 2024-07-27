package Utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

public class BTreeImpl {

    static final boolean DEBUG = false;
    static int NODE_ARITY = 12;
    static int OFFSET = 16;
    static int order = NODE_ARITY;
    static int NODE_POINTER_EMPTY = 8;
    static int KEY_LENGTH = 32;
    static int KEY_SIZE = KEY_LENGTH * NODE_ARITY;
    static int VALUE_LENGTH = 293;
    static int VALUE_SIZE = VALUE_LENGTH * NODE_ARITY;
    static int PARENT_SIZE = 8;
    static int NODE_SIZE = 4096;
    static int NUM_ELEMENTS_SIZE = 4;
    public Element root;
    public int TREE_SIZE = 0;
    public LinkedList<Long> emptyNodes = new LinkedList<Long>();
    public Map<String, Element> nodeCache;
    public boolean loading = false;
    static int PAD = NODE_SIZE - PARENT_SIZE - NUM_ELEMENTS_SIZE - KEY_SIZE - VALUE_SIZE;
    RandomAccessFile file;
    int removed = 0;

    public BTreeImpl(RandomAccessFile file) {
        try {
            this.file = file;
            if (file.length() == 0) {
                file.seek(0);
                file.writeInt(TREE_SIZE);
                file.writeInt(NODE_ARITY);
                file.writeLong(-1);
            } else {
                root = new Element();
                root = root.readElement(OFFSET);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        Map modLinkHashMap = new LinkedHashMap<String, Element>(200) {

            private static final int ENTRIES = 1000;

            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > ENTRIES;
            }
        };
        nodeCache = modLinkHashMap;
    }

    public boolean add(String key, String data) {

        if (key == null) {
            return false;
        }

        try {
            if (file.length() <= OFFSET) {
                root = new Element(key, data);
                root.parent = -1;
                file.seek(OFFSET);
                root.writeData(root, -1);
            } else {
                boolean isSplitNeeded = root.set(key, data);

                if (isSplitNeeded) {
                    root.writeData(root, -1);
                    root.splitRoot();
                }

                file.seek(OFFSET);
                root.writeData(root, -1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        this.TREE_SIZE++;
        return true;
    }

    private class Element {

        List<String> values;
        List<String> keys;
        long parent;

        public Element(String key, String value) {
            this.values = new LinkedList();
            this.keys = new LinkedList();
            this.values.add(value);
            this.keys.add(key);
        }

        public Element() {
            this.parent = -1;
            this.values = new LinkedList();
            this.keys = new LinkedList();
        }


        public long removeEmptyNodePointer() throws Exception {
            file.seek(NODE_POINTER_EMPTY);
            long emptyNodeAddress = file.readLong();

            if (emptyNodeAddress != -1) {
                Element emptyNode = fetchElement(emptyNodeAddress);
                String firstValue = emptyNode.values.get(0);
                long nextEmptyNodeAddress;

                if (!firstValue.equals("$END")) {
                    nextEmptyNodeAddress = extractPointerValue(firstValue);
                } else {
                    nextEmptyNodeAddress = -1;
                }
                ;

                file.seek(NODE_POINTER_EMPTY);
                file.writeLong(nextEmptyNodeAddress);
            }

            return emptyNodeAddress;
        }

        public void writeData(Element element, long currentSeek) {
            try {
                if (element.parent == -1 || currentSeek == -1) {
                    file.seek(OFFSET);
                    file.writeLong(-1);
                } else {
                    file.seek(currentSeek);
                    file.writeLong(element.parent);
                }
                int elementKeyCount = element.keys.size();
                file.writeInt(elementKeyCount);
                for (String key : element.keys) {
                    writePaddedData(key, KEY_LENGTH);
                }
                writePadding((NODE_ARITY - elementKeyCount) * KEY_LENGTH);
                for (String value : element.values) {
                    writePaddedData(value, VALUE_LENGTH);
                }
                writePadding((NODE_ARITY - elementKeyCount) * VALUE_LENGTH);

                writePadding(PAD);

                nodeCache.put(currentSeek + "", element);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void writePaddedData(String data, int length) throws IOException {
            byte[] bytes = data.getBytes();
            file.write(bytes);

            int paddingLength = length - bytes.length;
            writePadding(paddingLength);
        }

        private void writePadding(int length) throws IOException {
            byte[] padding = new byte[length];

            Arrays.fill(padding, (byte) ' ');
            file.write(padding);
        }

        public Element readElement(long filePointer) {
            try {
                Element element = new Element();
                file.seek(filePointer);
                element.parent = file.readLong();
                int elementCount = file.readInt();
                long keyStartPosition = file.getFilePointer();

                for (int i = 0; i < elementCount; i++) {
                    byte[] keyBytes = new byte[KEY_LENGTH];
                    file.readFully(keyBytes, 0, KEY_LENGTH);
                    String key = new String(keyBytes).trim();
                    element.keys.add(key);
                }

                long valueStartPosition = keyStartPosition + KEY_SIZE;
                file.seek(valueStartPosition);

                for (int i = 0; i < elementCount; i++) {
                    byte[] valueBytes = new byte[VALUE_LENGTH];
                    file.readFully(valueBytes, 0, VALUE_LENGTH);
                    String value = new String(valueBytes).trim();
                    element.values.add(value);
                }
                return element;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        public Element fetchElement(long address) {
            Element n;
            if (nodeCache.containsKey(address + "")) {
                n = nodeCache.get(address + "");
            } else {
                n = this.readElement(address);
            }
            return n;
        }

        public boolean hasChildNodes() {
            if (this.values.size() == 0) {
                return false;
            } else {
                return this.values.get(0).startsWith("$");
            }
        }

        public void splitRoot() {
            boolean hasChildren = hasChildNodes();
            boolean shouldSetRight = true;
            Element leftElement = new Element();
            Element rightElement = new Element();

            // simplify the methods used to obtain the pointers
            long leftElementPointer = getNextAvailablePointer();
            nodeCache.put(String.valueOf(leftElementPointer), leftElement);
            long rightElementPointer = getNextAvailablePointer();
            nodeCache.put(String.valueOf(rightElementPointer), rightElement);
            int midpoint = (int) Math.ceil(1.0 * BTreeImpl.order / 2);
            int keysSize = this.keys.size();
            // refactor loop: use iteration on list instead of for loop
            for (int i = 0; i < keysSize; i++) {
                if (i < midpoint) {
                    leftElement.set(this.keys.remove(0), this.values.remove(0));
                } else {
                    rightElement.set(this.keys.remove(0), this.values.remove(0));
                }
            }
            // if it has children, set the flag to false and modify the leftElement's last key
            if (hasChildren) {
                shouldSetRight = false;
                String createdLeftPointerLocation = formatPointerValue(leftElementPointer);
                this.set(leftElement.keys.set(leftElement.keys.size() - 1, "null"), createdLeftPointerLocation);
            }
            // set the parents for the new elements
            leftElement.parent = OFFSET;
            rightElement.parent = OFFSET;
            if (shouldSetRight) {
                String createdLeftPointerLocation = formatPointerValue(leftElementPointer);
                this.set(rightElement.keys.get(0), createdLeftPointerLocation);
            }
            // create the pointer location for the rightElement
            String createdRightPointerLocation = formatPointerValue(rightElementPointer);
            this.set("null", createdRightPointerLocation);
            // write data for the new elements
            writeData(leftElement, leftElementPointer);
            writeData(rightElement, rightElementPointer);
        }

        private String formatPointerValue(long pointerValue) {
            return "$" + pointerValue;
        }

        private long extractPointerValue(String formattedPointer) {
            if (formattedPointer.startsWith("$")) {
                return Long.parseLong(formattedPointer.substring(1));
            } else {
                return -99;
            }
        }

        private long getNextAvailablePointer() {
            try {
                long nextPointer = this.removeEmptyNodePointer();
                if (nextPointer == -1) {
                    long fileLength = file.length();
                    String fileLengthKey = Long.toString(fileLength);
                    while (nodeCache.containsKey(fileLengthKey)) {
                        fileLength += NODE_SIZE;
                        fileLengthKey = Long.toString(fileLength);
                    }
                    return fileLength;
                } else {
                    return nextPointer;
                }
            } catch (Exception exception) {
                exception.printStackTrace();
                return -1;
            }
        }

        private void splitLeafNode(Element right) {
            Element left = new Element();
            long leftPointer = this.getNextAvailablePointer();
            nodeCache.put(leftPointer + "", left);
            int half = (int) Math.ceil(1.0 * BTreeImpl.order / 2);
            for (int i = 0; i < half; i++) {
                left.set(right.keys.remove(0), right.values.remove(0));
            }
            this.set(right.keys.get(0), this.formatPointerValue(leftPointer));
            this.writeData(left, leftPointer);
        }

        private void splitInternalNode(Element right) {
            Element left = new Element();
            long leftPointer = this.getNextAvailablePointer();
            nodeCache.put(leftPointer + "", left);
            int half = (int) Math.ceil(1.0 * BTreeImpl.order / 2);
            for (int i = 0; i < half; i++) {
                left.set(right.keys.remove(0), right.values.remove(0));
            }
            String s = left.keys.set(left.keys.size() - 1, "null");
            this.set(s, this.formatPointerValue(leftPointer));
            this.writeData(left, leftPointer);
        }

        private boolean needToSplit() {
            return this.keys.size() > BTreeImpl.order - 1;
        }

        private void splitChild(Element n) {
            if (!n.hasChildNodes()) {
                this.splitLeafNode(n);
            } else {
                this.splitInternalNode(n);
            }
        }

        public boolean set(String key, String value) {
            if (keys.isEmpty()) {
                keys.add(key);
                values.add(value);
                return needToSplit();
            }

            if (!hasChildNodes()) {
                return addKeyAndValueInNonChildNodes(key, value);
            } else {
                return addKeyAndValueInChildrenNodes(key, value);
            }
        }

        private boolean addKeyAndValueInNonChildNodes(String key, String value) {
            for (int i = 0; i < keys.size(); i++) {
                int comparisonResult = keys.get(i).compareTo(key);
                if (key.equals("null") || key.equals(keys.get(i)) || comparisonResult < 0) {
                    keys.add(i, key);
                    values.add(i, value);
                    return needToSplit();
                }
            }
            keys.add(key);
            values.add(value);
            return needToSplit();
        }

        private boolean addKeyAndValueInChildrenNodes(String key, String value) {
            if (value.startsWith("$")) {
                return handleKeyAndValueStartingWithDollar(key, value);
            } else {
                return addKeyAndValueInRealChildNodes(key, value);
            }
        }

        private boolean handleKeyAndValueStartingWithDollar(String key, String value) {
            for (int i = 0; i < keys.size(); i++) {
                if (!keys.get(i).equals("null")) {
                    int comparisonResult = keys.get(i).compareTo(key);
                    if (key.equals(keys.get(i)) || comparisonResult < 0) {
                        keys.add(i, key);
                        values.add(i, value);
                        return needToSplit();
                    }
                }
            }

            if (keys.contains("null")) {
                keys.add(keys.size() - 1, key);
                values.add(values.size() - 1, value);
                return needToSplit();
            }

            keys.add(key);
            values.add(value);
            return needToSplit();
        }

        private boolean addKeyAndValueInRealChildNodes(String key, String value) {
            for (int i = 0; i < keys.size() - 1; i++) {
                int comparison = this.keys.get(i).compareTo(key);
                if (key.equals(keys.get(i)) || comparison < 0) {
                    return processChildNode(i, key, value);
                }
            }
            return processLastChildNode(key, value);
        }

        private boolean processChildNode(int index, String key, String value) {
            long pointerLocation = extractPointerValue(values.get(index));
            Element node = fetchElement(pointerLocation);
            if (node.set(key, value)) {
                splitChild(node);
            }
            writeData(node, pointerLocation);
            return needToSplit();
        }

        private boolean processLastChildNode(String key, String value) {
            long pointerLocation = extractPointerValue(values.get(keys.size() - 1));
            Element node = fetchElement(pointerLocation);
            if (node.set(key, value)) {
                splitChild(node);
            }
            writeData(node, pointerLocation);
            return needToSplit();
        }
    }
}
