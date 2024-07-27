package Utils;

import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Date;
import Constants.Constants;

public class BPlusTreeImpl {

    public static int getMiddleKey(RandomAccessFile fileObj, int pageNo) {
        int value = 0;
        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE);
            byte pageType = fileObj.readByte();
            int numCells = getCellId(fileObj, pageNo);
            int mid = (int) Math.ceil((double) numCells / 2);
            long loc = getCellPos(fileObj, pageNo, mid - 1);
            fileObj.seek(loc);

            if (pageType == Constants.SHORT_INT_VALUE) {
                fileObj.readInt();
                value = fileObj.readInt();
            } else if (pageType == Constants.PAGE_RECORDS) {
                fileObj.readShort();
                value = fileObj.readInt();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return value;
    }


    public static void splitChildPage(RandomAccessFile fileObj, int currentPageNo, int newPageNo) {
        try {

            int numberOfCells = getCellId(fileObj, currentPageNo);

            int mid = (int) Math.ceil((double) numberOfCells / 2);

            int cellAPos = mid - 1;
            int cellBPos = numberOfCells - cellAPos;
            int size = 512;

            for (int i = cellAPos; i < numberOfCells; i++) {
                long loc = getCellPos(fileObj, currentPageNo, i);
                fileObj.seek(loc);
                int cellSize = fileObj.readShort() + 6;
                size = size - cellSize;
                fileObj.seek(loc);
                byte[] cell = new byte[cellSize];
                fileObj.read(cell);
                fileObj.seek((long) (newPageNo - 1) * Constants.PAGE_SIZE + size);
                fileObj.write(cell);
                setOffsetForCell(fileObj, newPageNo, i - cellAPos, size);
            }


            fileObj.seek((long) (newPageNo - 1) * Constants.PAGE_SIZE + 2);
            fileObj.writeShort(size);


            short cellOffset = getOffsetForCell(fileObj, currentPageNo, cellAPos - 1);
            fileObj.seek((long) (currentPageNo - 1) * Constants.PAGE_SIZE + 2);
            fileObj.writeShort(cellOffset);


            int rightMostNode = getRightMostKey(fileObj, currentPageNo);
            setRightMostKey(fileObj, newPageNo, rightMostNode);
            setRightMostKey(fileObj, currentPageNo, newPageNo);


            int root = getRoot(fileObj, currentPageNo);
            setRoot(fileObj, newPageNo, root);


            byte byteNumber = (byte) cellAPos;
            setCellId(fileObj, currentPageNo, byteNumber);
            byteNumber = (byte) cellBPos;
            setCellId(fileObj, newPageNo, byteNumber);

        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    public static void splitMiddlePage(RandomAccessFile fileObj, int currentPageNo, int newPageNo) {
        try {

            int numberOfCells = getCellId(fileObj, currentPageNo);

            int mid = (int) Math.ceil((double) numberOfCells / 2);

            int cellAPos = mid - 1;
            int cellBPos = numberOfCells - cellAPos - 1;
            short size = 512;

            for (int i = cellAPos + 1; i < numberOfCells; i++) {
                long loc = getCellPos(fileObj, currentPageNo, i);
                short cellSize = 8;
                size = (short) (size - cellSize);
                fileObj.seek(loc);
                byte[] cell = new byte[cellSize];
                fileObj.read(cell);
                fileObj.seek((long) (newPageNo - 1) * Constants.PAGE_SIZE + size);
                fileObj.write(cell);
                fileObj.seek(loc);
                int page = fileObj.readInt();
                setRoot(fileObj, page, newPageNo);
                setOffsetForCell(fileObj, newPageNo, i - (cellAPos + 1), size);
            }

            int rightMostTmp = getRightMostKey(fileObj, currentPageNo);
            setRightMostKey(fileObj, newPageNo, rightMostTmp);

            long middleCellLocation = getCellPos(fileObj, currentPageNo, mid - 1);
            fileObj.seek(middleCellLocation);
            rightMostTmp = fileObj.readInt();
            setRightMostKey(fileObj, currentPageNo, rightMostTmp);

            fileObj.seek((long) (newPageNo - 1) * Constants.PAGE_SIZE + 2);
            fileObj.writeShort(size);

            short cellOffset = getOffsetForCell(fileObj, currentPageNo, cellAPos - 1);
            fileObj.seek((long) (currentPageNo - 1) * Constants.PAGE_SIZE + 2);
            fileObj.writeShort(cellOffset);


            int root = getRoot(fileObj, currentPageNo);
            setRoot(fileObj, newPageNo, root);

            byte num = (byte) cellAPos;
            setCellId(fileObj, currentPageNo, num);
            num = (byte) cellBPos;
            setCellId(fileObj, newPageNo, num);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Integer divide(RandomAccessFile fileObj, int pageNo, int newPageNo, int middleKeyNo, int rootNode) {
        if (rootNode == 0) {
            int rootPage = createInteriorPage(fileObj);
            setRoot(fileObj, pageNo, rootPage);
            setRoot(fileObj, newPageNo, rootPage);
            setRightMostKey(fileObj, rootPage, newPageNo);
            addInteriorCell(fileObj, rootPage, pageNo, middleKeyNo);
            return rootPage;
        } else {
            long ploc = fetchPointerLocation(fileObj, pageNo, rootNode);
            fixPointerLocation(fileObj, ploc, rootNode, newPageNo);
            addInteriorCell(fileObj, rootNode, pageNo, middleKeyNo);
            sortCells(fileObj, rootNode);
            return rootNode;
        }
    }

    public static void splitChild(RandomAccessFile fileObj, int pageNo) {
        int newPageNo = makeChildPage(fileObj);
        int middleKey = getMiddleKey(fileObj, pageNo);
        splitChildPage(fileObj, pageNo, newPageNo);
        int root = getRoot(fileObj, pageNo);

        divide(fileObj, pageNo, newPageNo, middleKey, root);
        if (root != 0) {
            while (isValidInnerSpace(fileObj, root)) {
                root = splitMiddle(fileObj, root);
            }
        }
    }

    public static int splitMiddle(RandomAccessFile fileObj, int pageNo) {
        int newPageNo = createInteriorPage(fileObj);
        int middleKeyNo = getMiddleKey(fileObj, pageNo);
        splitMiddlePage(fileObj, pageNo, newPageNo);
        int root = getRoot(fileObj, pageNo);

        return divide(fileObj, pageNo, newPageNo, middleKeyNo, root);
    }

    public static void switchNodes(int[] nodesArray, int indexA, int indexB) {
        int temp = nodesArray[indexA];
        nodesArray[indexA] = nodesArray[indexB];
        nodesArray[indexB] = temp;
    }

    public static void switchNodes(short[] nodesArray, int indexA, int indexB) {
        short temp = nodesArray[indexA];
        nodesArray[indexA] = nodesArray[indexB];
        nodesArray[indexB] = temp;
    }

    public static void sortCells(RandomAccessFile fileObj, int pageNo) {
        byte num = getCellId(fileObj, pageNo);
        int[] keyArray = getArrayOfKey(fileObj, pageNo);
        short[] cellArray = getCells(fileObj, pageNo);

        for (int i = 1; i < num; i++) {
            for (int j = i; j > 0; j--) {
                if (keyArray[j] < keyArray[j - 1]) {
                    switchNodes(keyArray, j, j - 1);
                    switchNodes(cellArray, j, j - 1);
                }
            }
        }

        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 12);
            for (int i = 0; i < num; i++) {
                fileObj.writeShort(cellArray[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int[] getArrayOfKey(RandomAccessFile fileObj, int pageNo) {
        int cellNo = Integer.valueOf(getCellId(fileObj, pageNo));
        int[] keys = new int[cellNo];

        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE);
            byte bytePageType = fileObj.readByte();
            byte byteOffset = 0;

            if (bytePageType == Constants.SHORT_INT_VALUE) {
                byteOffset = 4;
            } else {
                byteOffset = 2;
            }

            for (int i = 0; i < cellNo; i++) {
                long locationNo = getCellPos(fileObj, pageNo, i);
                fileObj.seek(locationNo + byteOffset);
                keys[i] = fileObj.readInt();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return keys;
    }

    public static short[] getCells(RandomAccessFile fileObj, int pageNo) {
        int cellNo = Integer.valueOf(getCellId(fileObj, pageNo));
        short[] cells = new short[cellNo];

        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 12);
            for (int i = 0; i < cellNo; i++) {
                cells[i] = fileObj.readShort();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return cells;
    }


    public static long fetchPointerLocation(RandomAccessFile fileObj, int pageNo, int root) {
        long pointerValue = 0;
        try {
            int cellNumber = Integer.valueOf(getCellId(fileObj, root));
            for (int i = 0; i < cellNumber; i++) {
                long location = getCellPos(fileObj, root, i);
                fileObj.seek(location);
                int childPage = fileObj.readInt();
                if (childPage == pageNo) {
                    pointerValue = location;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return pointerValue;
    }

    public static void fixPointerLocation(RandomAccessFile fileObj, long location, int root, int pageNo) {
        try {
            if (location == 0) {
                fileObj.seek((long) (root - 1) * Constants.PAGE_SIZE + 4);
            } else {
                fileObj.seek(location);
            }
            fileObj.writeInt(pageNo);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void addInteriorCell(RandomAccessFile fileObj, int pageNo, int childPoc, int keyPos) {
        try {

            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 2);
            short cellValue = fileObj.readShort();

            if (cellValue == 0) cellValue = 512;

            cellValue = (short) (cellValue - 8);

            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + cellValue);
            fileObj.writeInt(childPoc);
            fileObj.writeInt(keyPos);

            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 2);
            fileObj.writeShort(cellValue);

            byte cellNo = getCellId(fileObj, pageNo);
            setOffsetForCell(fileObj, pageNo, cellNo, cellValue);

            cellNo = (byte) (cellNo + 1);
            setCellId(fileObj, pageNo, cellNo);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insertChildCell(RandomAccessFile fileObj, int pageNo, int offsetValue, short pSize, int keyValue, byte[] byteArray, String[] values) {
        try {
            updateChildCell(fileObj, pageNo, offsetValue, pSize, keyValue, byteArray, values);

            int n = getCellId(fileObj, pageNo);
            byte tmp = (byte) (n + 1);
            setCellId(fileObj, pageNo, tmp);
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 12 + n * 2);
            fileObj.writeShort(offsetValue);
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 2);
            int fileContent = fileObj.readShort();
            if (fileContent >= offsetValue || fileContent == 0) {
                fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 2);
                fileObj.writeShort(offsetValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void updateChildCell(RandomAccessFile fileObj, int pageNo, int offsetValue, int pSize, int keyValue, byte[] byteArray, String[] values) {
        try {
            String dateValue;
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + offsetValue);
            fileObj.writeShort(pSize);
            fileObj.writeInt(keyValue);
            int col = values.length - 1;
            fileObj.writeByte(col);
            fileObj.write(byteArray);
            for (int i = 1; i < values.length; i++) {
                switch (byteArray[i - 1]) {
                    case Constants.NULL_VALUE:
                        fileObj.writeByte(0);
                        break;
                    case Constants.SHORT_NULL:
                        fileObj.writeShort(0);
                        break;
                    case Constants.INTEGER_NULL:
                        fileObj.writeInt(0);
                        break;
                    case Constants.LONG_NULL:
                        fileObj.writeLong(0);
                        break;
                    case Constants.TINY_INT_VALUE:
                        fileObj.writeByte(Byte.valueOf(values[i]));
                        break;
                    case Constants.SHORT_INT_VALUE:
                        fileObj.writeShort(Short.valueOf(values[i]));
                        break;
                    case Constants.INTEGER_VALUE:
                        fileObj.writeInt(Integer.valueOf(values[i]));
                        break;
                    case Constants.LONG_VALUE:
                        fileObj.writeLong(Long.valueOf(values[i]));
                        break;
                    case Constants.FLOAT_VALUE:
                        fileObj.writeFloat(new Float(values[i]));
                        break;
                    case Constants.DOUBLE_VALUE:
                        fileObj.writeDouble(new Double(values[i]));
                        break;
                    case Constants.DATETIME_VALUE:
                        dateValue = values[i];
                        Date temp = new SimpleDateFormat(Constants.TIMESTAMP_PATTERN).parse(dateValue.substring(1, dateValue.length() - 1));
                        long timeValue = temp.getTime();
                        fileObj.writeLong(timeValue);
                        break;
                    case Constants.DATE_VALUE:
                        dateValue = values[i];
                        dateValue = dateValue.substring(1, dateValue.length() - 1);
                        dateValue = dateValue + "_00:00:00";
                        Date temp2 = new SimpleDateFormat(Constants.TIMESTAMP_PATTERN).parse(dateValue);
                        long finalTime = temp2.getTime();
                        fileObj.writeLong(finalTime);
                        break;
                    default:
                        fileObj.writeBytes(values[i]);
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int getRoot(RandomAccessFile fileObj, int pageNo) {
        int value = 0;

        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 8);
            value = fileObj.readInt();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return value;
    }

    public static void setRoot(RandomAccessFile fileObj, int pageNo, int root) {
        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 8);
            fileObj.writeInt(root);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int getRightMostKey(RandomAccessFile fileObj, int pageNo) {
        int rightMostKeyValue = 0;

        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 4);
            rightMostKeyValue = fileObj.readInt();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return rightMostKeyValue;
    }

    public static void setRightMostKey(RandomAccessFile fileObj, int pageNo, int rightChildPos) {

        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 4);
            fileObj.writeInt(rightChildPos);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static boolean isKeyPresent(RandomAccessFile fileObj, int pageNo, int keyValue) {
        int[] keysArray = getArrayOfKey(fileObj, pageNo);
        for (int i : keysArray)
            if (keyValue == i) return true;
        return false;
    }

    public static long getCellPos(RandomAccessFile fileObj, int pageNO, int cellPos) {
        long cellLoc = 0;
        try {
            fileObj.seek((long) (pageNO - 1) * Constants.PAGE_SIZE + 12 + cellPos * 2L);
            short offset = fileObj.readShort();
            long orig = (long) (pageNO - 1) * Constants.PAGE_SIZE;
            cellLoc = orig + offset;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cellLoc;
    }

    public static byte getCellId(RandomAccessFile fileObj, int pageNo) {
        byte byteValue = 0;

        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 1);
            byteValue = fileObj.readByte();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return byteValue;
    }

    public static void setCellId(RandomAccessFile fileObj, int pageNo, byte byteValue) {
        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 1);
            fileObj.writeByte(byteValue);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static short getOffsetForCell(RandomAccessFile fileObj, int pageNo, int cellPos) {
        short offsetValue = 0;
        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 12 + cellPos * 2L);
            offsetValue = fileObj.readShort();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return offsetValue;
    }

    public static void setOffsetForCell(RandomAccessFile fileObj, int pageNo, int cellPos, int cellOffset) {
        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 12 + cellPos * 2L);
            fileObj.writeShort(cellOffset);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte getTypeOfPage(RandomAccessFile fileObj, int pageNo) {
        byte byteType = Constants.SHORT_INT_VALUE;
        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE);
            byteType = fileObj.readByte();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return byteType;
    }

    public static int createPage(RandomAccessFile fileObj, int byteValue) {
        int numberOfPages = 0;
        try {
            numberOfPages = (int) (fileObj.length() / (Long.valueOf(Constants.PAGE_SIZE)));
            numberOfPages = numberOfPages + 1;
            fileObj.setLength((long) Constants.PAGE_SIZE * numberOfPages);
            fileObj.seek((long) (numberOfPages - 1) * Constants.PAGE_SIZE);
            fileObj.writeByte(byteValue);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return numberOfPages;
    }

    public static int createInteriorPage(RandomAccessFile fileObj) {
        return createPage(fileObj, Constants.SHORT_INT_VALUE);
    }

    public static int makeChildPage(RandomAccessFile fileObj) {
        return createPage(fileObj, Constants.PAGE_RECORDS);
    }

    public static boolean isValidInnerSpace(RandomAccessFile fileObj, int pageNo) {
        byte cellBytes = getCellId(fileObj, pageNo);
        return cellBytes > 30;
    }

    public static int getChildSpace(RandomAccessFile fileObj, int pageNo, int pageSize) {
        int value = -1;
        try {
            fileObj.seek((long) (pageNo - 1) * Constants.PAGE_SIZE + 2);
            int fileContent = fileObj.readShort();
            if (fileContent == 0) return Constants.PAGE_SIZE - pageSize;
            int noOfCells = getCellId(fileObj, pageNo);
            int spaceBetween = fileContent - 20 - 2 * noOfCells;
            if (pageSize < spaceBetween) return fileContent - pageSize;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }
}