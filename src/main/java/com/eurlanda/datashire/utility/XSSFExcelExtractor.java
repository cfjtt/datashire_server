package com.eurlanda.datashire.utility;

import org.apache.poi.POIXMLTextExtractor;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.HeaderFooter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRelation;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.xmlbeans.XmlException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class XSSFExcelExtractor extends POIXMLTextExtractor implements
		org.apache.poi.ss.extractor.ExcelExtractor {
	public static final XSSFRelation[] SUPPORTED_TYPES = {
			XSSFRelation.WORKBOOK, XSSFRelation.MACRO_TEMPLATE_WORKBOOK,
			XSSFRelation.MACRO_ADDIN_WORKBOOK, XSSFRelation.TEMPLATE_WORKBOOK,
			XSSFRelation.MACROS_WORKBOOK };
	private XSSFWorkbook workbook;
	private boolean includeSheetNames = true;
	private boolean formulasNotResults = false;
	private boolean includeCellComments = false;
	private boolean includeHeadersFooters = true;
	private boolean includeBlankCells = true;

	public XSSFExcelExtractor(String path) throws XmlException,
			OpenXML4JException, IOException {
		this(new XSSFWorkbook(path));
	}

	public XSSFExcelExtractor(OPCPackage container) throws XmlException,
			OpenXML4JException, IOException {
		this(new XSSFWorkbook(container));
	}

	public XSSFExcelExtractor(XSSFWorkbook workbook) {
		super(workbook);
		this.workbook = workbook;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Use:");
			System.err.println("  XSSFExcelExtractor <filename.xlsx>");
			System.exit(1);
		}
		POIXMLTextExtractor extractor = new XSSFExcelExtractor(args[0]);

		System.out.println(extractor.getText());
	}

	public void setIncludeSheetNames(boolean includeSheetNames) {
		this.includeSheetNames = includeSheetNames;
	}

	public void setFormulasNotResults(boolean formulasNotResults) {
		this.formulasNotResults = formulasNotResults;
	}

	public void setIncludeCellComments(boolean includeCellComments) {
		this.includeCellComments = includeCellComments;
	}

	public void setIncludeHeadersFooters(boolean includeHeadersFooters) {
		this.includeHeadersFooters = includeHeadersFooters;
	}

	public List<List<String>> getRowListCellList() {
		List<List<String>> excelList = new ArrayList<List<String>>();
		List<String> text = null;
		for (int i = 0; i < this.workbook.getNumberOfSheets(); i++) {
			XSSFSheet sheet = this.workbook.getSheetAt(i);
			for (Object rawR : sheet) {
				Row row = (Row) rawR;
				int index = 0;
				text = new ArrayList<String>();
				for (Iterator ri = row.cellIterator(); ri.hasNext();) {
					Cell cell = (Cell) ri.next();
					if (includeBlankCells) {
						while (index < cell.getColumnIndex()) {
							if (StringUtils.isNotNull(text)) {
								text.add("");
							}
							index++;
						}
					}
					if ((cell.getCellType() == 2) && (this.formulasNotResults)) {
						text.add(cell.getCellFormula());
					} else if (cell.getCellType() == 1) {
						text.add(cell.getRichStringCellValue().getString());
					} else {
						XSSFCell xc = (XSSFCell) cell;
						text.add(xc.getRawValue());
					}
					index++;
				}
				excelList.add(text);
			}
		}
		return excelList;
	}

	public List<String> getList() {
		List<String> excelList = new ArrayList<String>();
		StringBuffer text = null;

		for (int i = 0; i < this.workbook.getNumberOfSheets(); i++) {
			XSSFSheet sheet = this.workbook.getSheetAt(i);
			text = new StringBuffer();
			if (this.includeSheetNames) {
				text.append(this.workbook.getSheetName(i)).append("\n");
			}

			if (this.includeHeadersFooters) {
				text.append(extractHeaderFooter(sheet.getFirstHeader()));

				text.append(extractHeaderFooter(sheet.getOddHeader()));

				text.append(extractHeaderFooter(sheet.getEvenHeader()));
			}
			if (StringUtils.isNotNull(text)) {
				excelList.add(text.toString());
			}
			int count = 0;
			for (Object rawR : sheet) {
				text = new StringBuffer();
				Row row = (Row) rawR;
				int index = 0;
				for (Iterator ri = row.cellIterator(); ri.hasNext();) {
					Cell cell = (Cell) ri.next();

					if (includeBlankCells) {
						while (index < cell.getColumnIndex()) {
							text.append("\t");
							index++;
						}
					}

					if ((cell.getCellType() == 2) && (this.formulasNotResults)) {
						text.append(cell.getCellFormula());
					} else if (cell.getCellType() == 1) {
						text.append(cell.getRichStringCellValue().getString());
					} else {
						XSSFCell xc = (XSSFCell) cell;
						text.append(StringUtils.isNull(xc.getRawValue())?"":xc.getRawValue());
					}

					Comment comment = cell.getCellComment();
					if ((this.includeCellComments) && (comment != null)) {
						String commentText = comment.getString().getString()
								.replace('\n', ' ');
						text.append(" Comment by ").append(comment.getAuthor())
								.append(": ").append(commentText);
					}

					if (ri.hasNext())
						text.append("\t");
					index++;
				}
				count++;
				// text.append("\n");
				// System.out.println(count + ":" + text.toString());
				excelList.add(text.toString());
			}
			text = new StringBuffer();
			if (this.includeHeadersFooters) {
				text.append(extractHeaderFooter(sheet.getFirstFooter()));

				text.append(extractHeaderFooter(sheet.getOddFooter()));

				text.append(extractHeaderFooter(sheet.getEvenFooter()));
			}
			if (StringUtils.isNotNull(text)) {
				excelList.add(text.toString());
			}
		}

		return excelList;
	}


	/**
	 * 获取指定行数的文件内容
	 */
	public List<String> getList(int No){
			List<String> excelList = new ArrayList<String>();
			StringBuffer text = null;
			for (int i = 0; i < this.workbook.getNumberOfSheets(); i++) {
				XSSFSheet sheet = this.workbook.getSheetAt(i);
				text = new StringBuffer();
				if (this.includeSheetNames) {
					text.append(this.workbook.getSheetName(i)).append("\n");
				}

				if (this.includeHeadersFooters) {
					text.append(extractHeaderFooter(sheet.getFirstHeader()));

					text.append(extractHeaderFooter(sheet.getOddHeader()));

					text.append(extractHeaderFooter(sheet.getEvenHeader()));
				}
				if (StringUtils.isNotNull(text)) {
					excelList.add(text.toString());
				}
				int count = 0;
				for (Object rawR : sheet) {
					text = new StringBuffer();
					Row row = (Row) rawR;
					int index = 0;
					if(count!=No){
						continue;
					}
					if(count==No){
						break;
					}
					for (Iterator ri = row.cellIterator(); ri.hasNext();) {
						Cell cell = (Cell) ri.next();

						if (includeBlankCells) {
							while (index < cell.getColumnIndex()) {
								text.append("\t");
								index++;
							}
						}

						if ((cell.getCellType() == 2) && (this.formulasNotResults)) {
							text.append(cell.getCellFormula());
						} else if (cell.getCellType() == 1) {
							text.append(cell.getRichStringCellValue().getString());
						} else {
							XSSFCell xc = (XSSFCell) cell;
							text.append(StringUtils.isNull(xc.getRawValue())?"":xc.getRawValue());
						}

						Comment comment = cell.getCellComment();
						if ((this.includeCellComments) && (comment != null)) {
							String commentText = comment.getString().getString()
									.replace('\n', ' ');
							text.append(" Comment by ").append(comment.getAuthor())
									.append(": ").append(commentText);
						}

						if (ri.hasNext())
							text.append("\t");
						index++;
					}
					count++;
					// text.append("\n");
					// System.out.println(count + ":" + text.toString());
					excelList.add(text.toString());
				}
				text = new StringBuffer();
				if (this.includeHeadersFooters) {
					text.append(extractHeaderFooter(sheet.getFirstFooter()));

					text.append(extractHeaderFooter(sheet.getOddFooter()));

					text.append(extractHeaderFooter(sheet.getEvenFooter()));
				}
				if (StringUtils.isNotNull(text)) {
					excelList.add(text.toString());
				}
			}
			return excelList;
	}
	/**
	 * 读取指定行数的xls文件
	 * @return
     */
	public List<String> getList(int firstRow,int lastRow){
		List<String> excelList = new ArrayList<String>();
		StringBuffer text = null;
		for (int i = 0; i < this.workbook.getNumberOfSheets(); i++) {
			XSSFSheet sheet = this.workbook.getSheetAt(i);
			text = new StringBuffer();
			if (this.includeSheetNames) {
				text.append(this.workbook.getSheetName(i)).append("\n");
			}

			if (this.includeHeadersFooters) {
				text.append(extractHeaderFooter(sheet.getFirstHeader()));

				text.append(extractHeaderFooter(sheet.getOddHeader()));

				text.append(extractHeaderFooter(sheet.getEvenHeader()));
			}
			if (StringUtils.isNotNull(text)) {
				excelList.add(text.toString());
			}
			int count = 0;
			for (Object rawR : sheet) {
				if(count>lastRow){
					break;
				}
				if(count<firstRow){
					continue;
				}
				text = new StringBuffer();
				Row row = (Row) rawR;
				int index = 0;
				for (Iterator ri = row.cellIterator(); ri.hasNext();) {
					Cell cell = (Cell) ri.next();

					if (includeBlankCells) {
						while (index < cell.getColumnIndex()) {
							text.append("\t");
							index++;
						}
					}

					if ((cell.getCellType() == 2) && (this.formulasNotResults)) {
						text.append(cell.getCellFormula());
					} else if (cell.getCellType() == 1) {
						text.append(cell.getRichStringCellValue().getString());
					} else {
						XSSFCell xc = (XSSFCell) cell;
						text.append(StringUtils.isNull(xc.getRawValue())?"":xc.getRawValue());
					}

					Comment comment = cell.getCellComment();
					if ((this.includeCellComments) && (comment != null)) {
						String commentText = comment.getString().getString()
								.replace('\n', ' ');
						text.append(" Comment by ").append(comment.getAuthor())
								.append(": ").append(commentText);
					}

					if (ri.hasNext())
						text.append("\t");
					index++;
				}
				count++;
				// text.append("\n");
				// System.out.println(count + ":" + text.toString());
				excelList.add(text.toString());
			}
			text = new StringBuffer();
			if (this.includeHeadersFooters) {
				text.append(extractHeaderFooter(sheet.getFirstFooter()));

				text.append(extractHeaderFooter(sheet.getOddFooter()));

				text.append(extractHeaderFooter(sheet.getEvenFooter()));
			}
			if (StringUtils.isNotNull(text)) {
				excelList.add(text.toString());
			}
		}

		return excelList;
	}

	/**
	 * 获取总的行数
	 * @return
     */
	public int getRowNo() {
		List<String> excelList = new ArrayList<String>();
		StringBuffer text = null;
		for (int i = 0; i < this.workbook.getNumberOfSheets(); i++) {
			XSSFSheet sheet = this.workbook.getSheetAt(i);
			if(sheet==null){
				continue;
			}
			int no = sheet.getLastRowNum();
			return no;
		}
		return 0;
	}
	/**
	 * 读取xls文件全部内容
	 * @return
     */
	public String getText() {

		StringBuffer text = new StringBuffer();

		for (int i = 0; i < this.workbook.getNumberOfSheets(); i++) {
			XSSFSheet sheet = this.workbook.getSheetAt(i);
			if (this.includeSheetNames) {
				text.append(this.workbook.getSheetName(i)).append("\n");
			}

			if (this.includeHeadersFooters) {
				text.append(extractHeaderFooter(sheet.getFirstHeader()));
				text.append(extractHeaderFooter(sheet.getOddHeader()));
				text.append(extractHeaderFooter(sheet.getEvenHeader()));
			}

			for (Object rawR : sheet) {
				Row row = (Row) rawR;
				int index = 0;
				for (Iterator ri = row.cellIterator(); ri.hasNext();) {
					Cell cell = (Cell) ri.next();

					if (includeBlankCells) {
						while (index < cell.getColumnIndex()) {
							text.append("\t");
							index++;
						}
					}

					if ((cell.getCellType() == 2) && (this.formulasNotResults)) {
						text.append(cell.getCellFormula());
					} else if (cell.getCellType() == 1) {
						text.append(cell.getRichStringCellValue().getString());
					} else {
						XSSFCell xc = (XSSFCell) cell;
						text.append(xc.getRawValue());
					}

					Comment comment = cell.getCellComment();
					if ((this.includeCellComments) && (comment != null)) {
						String commentText = comment.getString().getString().replace('\n', ' ');
						text.append(" Comment by ").append(comment.getAuthor()).append(": ").append(commentText);
					}

					if (ri.hasNext())
						text.append("\t");
					index++;
				}
				text.append("\n");
			}

			if (this.includeHeadersFooters) {
				text.append(extractHeaderFooter(sheet.getFirstFooter()));
				text.append(extractHeaderFooter(sheet.getOddFooter()));
				text.append(extractHeaderFooter(sheet.getEvenFooter()));
			}
		}

		return text.toString();
	}

	/**
	 * 读取从指定行到指定行的文件内容
	 * @return
     */
	public String getText(int No){

		StringBuffer text = new StringBuffer();

		for (int i = 0; i < this.workbook.getNumberOfSheets(); i++) {
			XSSFSheet sheet = this.workbook.getSheetAt(i);
			if (this.includeSheetNames) {
				text.append(this.workbook.getSheetName(i)).append("\n");
			}

			if (this.includeHeadersFooters) {
				text.append(extractHeaderFooter(sheet.getFirstHeader()));
				text.append(extractHeaderFooter(sheet.getOddHeader()));
				text.append(extractHeaderFooter(sheet.getEvenHeader()));
			}
			int count=0;
			for (Object rawR : sheet) {

				if(count>No){
					break;
				}
				count++;
				Row row = (Row) rawR;
				int index = 0;
				for (Iterator ri = row.cellIterator(); ri.hasNext();) {
					Cell cell = (Cell) ri.next();

					if (includeBlankCells) {
						while (index < cell.getColumnIndex()) {
							text.append("\t");
							index++;
						}
					}

					if ((cell.getCellType() == 2) && (this.formulasNotResults)) {
						text.append(cell.getCellFormula());
					} else if (cell.getCellType() == 1) {
						text.append(cell.getRichStringCellValue().getString());
					} else {
						XSSFCell xc = (XSSFCell) cell;
						text.append(xc.getRawValue());
					}

					Comment comment = cell.getCellComment();
					if ((this.includeCellComments) && (comment != null)) {
						String commentText = comment.getString().getString().replace('\n', ' ');
						text.append(" Comment by ").append(comment.getAuthor()).append(": ").append(commentText);
					}

					if (ri.hasNext())
						text.append("\t");
					index++;
				}
				text.append("\n");
			}

			if (this.includeHeadersFooters) {
				text.append(extractHeaderFooter(sheet.getFirstFooter()));
				text.append(extractHeaderFooter(sheet.getOddFooter()));
				text.append(extractHeaderFooter(sheet.getEvenFooter()));
			}
		}

		return text.toString();
	}

	private String extractHeaderFooter(HeaderFooter hf) {
		return org.apache.poi.hssf.extractor.ExcelExtractor._extractHeaderFooter(hf);
	}

	public void setIncludeBlankCells(boolean includeBlankCells) {
		this.includeBlankCells = includeBlankCells;
	}
}