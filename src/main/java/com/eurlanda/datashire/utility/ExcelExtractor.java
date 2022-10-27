package com.eurlanda.datashire.utility;

import org.apache.poi.POIOLE2TextExtractor;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFComment;
import org.apache.poi.hssf.usermodel.HSSFDataFormatter;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.formula.eval.ErrorEval;
import org.apache.poi.ss.usermodel.HeaderFooter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

//import org.apache.poi.hssf.record.formula.eval.ErrorEval;

public class ExcelExtractor extends POIOLE2TextExtractor
  implements org.apache.poi.ss.extractor.ExcelExtractor
{
  private HSSFWorkbook _wb;
  private HSSFDataFormatter _formatter;
  private boolean _includeSheetNames = true;
  private boolean _shouldEvaluateFormulas = true;
  private boolean _includeCellComments = false;
  private boolean _includeBlankCells = true;
  private boolean _includeHeadersFooters = true;

  public ExcelExtractor(HSSFWorkbook wb) {
    super(wb);
    this._wb = wb;
    this._formatter = new HSSFDataFormatter();
  }

  public ExcelExtractor(POIFSFileSystem fs) throws IOException {
    this(fs.getRoot(), fs);
  }
  public ExcelExtractor(DirectoryNode dir, POIFSFileSystem fs) throws IOException {
    this(new HSSFWorkbook(dir, fs, true));
  }

  private static void printUsageMessage(PrintStream ps)
  {
    ps.println("Use:");
    ps.println("    " + ExcelExtractor.class.getName() + " [<flag> <value> [<flag> <value> [...]]] [-i <filename.xls>]");
    ps.println("       -i <filename.xls> specifies input file (default is to use stdin)");
    ps.println("       Flags can be set on or off by using the values 'Y' or 'N'.");
    ps.println("       Following are available flags and their default values:");
    ps.println("       --show-sheet-names  Y");
    ps.println("       --evaluate-formulas Y");
    ps.println("       --show-comments     N");
    ps.println("       --show-blanks       Y");
    ps.println("       --headers-footers   Y");
  }

  public static void main1(String[] args)
  {
    CommandArgs cmdArgs;
    try
    {
      cmdArgs = new CommandArgs(args);
    } catch (CommandParseException e) {
      System.err.println(e.getMessage());
      printUsageMessage(System.err);
      System.exit(1);
      return;
    }

    if (cmdArgs.isRequestHelp()) {
      printUsageMessage(System.out);
      return;
    }
    try
    {
      InputStream is;
      //InputStream is;
      if (cmdArgs.getInputFile() == null)
        is = System.in;
      else {
        is = new FileInputStream(cmdArgs.getInputFile());
      }
      HSSFWorkbook wb = new HSSFWorkbook(is);

      ExcelExtractor extractor = new ExcelExtractor(wb);
      extractor.setIncludeSheetNames(cmdArgs.shouldShowSheetNames());
      extractor.setFormulasNotResults(!cmdArgs.shouldEvaluateFormulas());
      extractor.setIncludeCellComments(cmdArgs.shouldShowCellComments());
      extractor.setIncludeBlankCells(cmdArgs.shouldShowBlankCells());
      extractor.setIncludeHeadersFooters(cmdArgs.shouldIncludeHeadersFooters());
      System.out.println(extractor.getText());
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
  public static void main(String[] args) throws IOException {
	  InputStream in = new FileInputStream("D:\\业务测试数据\\xls_imdb_test.xls");
		HSSFWorkbook workbook = new HSSFWorkbook(in);
		ExcelExtractor extractor = new ExcelExtractor(workbook);
		extractor.setFormulasNotResults(true);
		extractor.setIncludeSheetNames(false);
		for(int i=0;i<extractor.getList().size();i++)
		{
			System.out.println(extractor.getList().get(i));
		}
	
}

  public void setIncludeSheetNames(boolean includeSheetNames)
  {
    this._includeSheetNames = includeSheetNames;
  }

  public void setFormulasNotResults(boolean formulasNotResults)
  {
    this._shouldEvaluateFormulas = (!formulasNotResults);
  }

  public void setIncludeCellComments(boolean includeCellComments)
  {
    this._includeCellComments = includeCellComments;
  }

  public void setIncludeBlankCells(boolean includeBlankCells)
  {
    this._includeBlankCells = includeBlankCells;
  }

  public void setIncludeHeadersFooters(boolean includeHeadersFooters)
  {
    this._includeHeadersFooters = includeHeadersFooters;
  }

    public List<List<String>> getRowListCellList() {
        List<List<String>> excelList = new ArrayList<List<String>>();

        List<String> text = null;

        this._wb.setMissingCellPolicy(HSSFRow.RETURN_BLANK_AS_NULL);

        for (int i = 0; i < this._wb.getNumberOfSheets(); i++) {
            HSSFSheet sheet = this._wb.getSheetAt(i);
            if (sheet == null)
                continue;
            int firstRow = sheet.getFirstRowNum();
            int lastRow = sheet.getLastRowNum();
            int count = 0;
            for (int j = firstRow; j <= lastRow; j++) {
                HSSFRow row = sheet.getRow(j);
                if (row == null) {
                    continue;
                }
                int firstCell = row.getFirstCellNum();
                int lastCell = row.getLastCellNum();
                if (this._includeBlankCells) {
                    firstCell = 0;
                }
                text = new ArrayList<String>();
                for (int k = firstCell; k < lastCell; k++) {
                    HSSFCell cell = row.getCell(k);
                    if (cell == null) {
                        text.add("");
                    } else {
                        switch (cell.getCellType()) {
                            case 1:
                                text.add(cell.getRichStringCellValue().getString());
                                break;
                            case 0:
                                text.add(this._formatter.formatCellValue(cell));

                                break;
                            case 4:
                                text.add(String.valueOf(cell.getBooleanCellValue()));
                                break;
                            case 5:
                                text.add(ErrorEval.getText(cell.getErrorCellValue()));
                                break;
                            case 2:
                                if (!this._shouldEvaluateFormulas)
                                    text.add(cell.getCellFormula());
                                else {
                                    switch (cell.getCachedFormulaResultType()) {
                                        case 1:
                                            HSSFRichTextString str = cell.getRichStringCellValue();
                                            if ((str == null) || (str.length() <= 0)) break;
                                            text.add(str.toString());
                                            break;
                                        case 0:
                                            HSSFCellStyle style = cell.getCellStyle();
                                            if (style == null)
                                                text.add(String.valueOf(cell.getNumericCellValue()));
                                            else {
                                                text.add(this._formatter.formatRawCellContents(cell.getNumericCellValue(), style.getDataFormat(), style.getDataFormatString()));
                                            }

                                            break;
                                        case 4:
                                            text.add(String.valueOf(cell.getBooleanCellValue()));
                                            break;
                                        case 5:
                                            text.add(ErrorEval.getText(cell.getErrorCellValue()));
                                        case 2:
                                        case 3:
                                    }
                                }
                                break;
                            case 3:
                            default:
                                throw new RuntimeException("Unexpected cell type (" + cell.getCellType() + ")");
                        }
                    }
                }

                count++;
                excelList.add(text);
            }
        }
        return excelList;
    }

  public List<String> getList(){
	    List<String> excelList = new ArrayList<String>();
	    
	    StringBuffer text = new StringBuffer();

	    this._wb.setMissingCellPolicy(HSSFRow.RETURN_BLANK_AS_NULL);

	    for (int i = 0; i < this._wb.getNumberOfSheets(); i++) {
	      HSSFSheet sheet = this._wb.getSheetAt(i);
	      if (sheet == null)
	        continue;
	      if (this._includeSheetNames) {
	        String name = this._wb.getSheetName(i);
	        if (name != null) {
	          text.append(name);
	          text.append("\n");
	        }

	      }

	      if (this._includeHeadersFooters) {
	        text.append(_extractHeaderFooter(sheet.getHeader()));
	      }

	      if(StringUtils.isNotNull(text)){
	    	  excelList.add(text.toString());
	      }
	      
	      int firstRow = sheet.getFirstRowNum();
	      int lastRow = sheet.getLastRowNum();
	      int count = 0;
	      for (int j = firstRow; j <= lastRow; j++) {
	        HSSFRow row = sheet.getRow(j);
	        if (row == null) {
	          continue;
	        }
	        int firstCell = row.getFirstCellNum();
	        int lastCell = row.getLastCellNum();
	        if (this._includeBlankCells) {
	          firstCell = 0;
	        }
	        text = new StringBuffer();
	        for (int k = firstCell; k < lastCell; k++) {
	          HSSFCell cell = row.getCell(k);
	          boolean outputContents = true;

	          if (cell == null)
	          {
	            outputContents = this._includeBlankCells;
	          } else {
	            switch (cell.getCellType()) {
	            case 1:
	              text.append(cell.getRichStringCellValue().getString());
	              break;
	            case 0:
	              text.append(this._formatter.formatCellValue(cell));

	              break;
	            case 4:
	              text.append(cell.getBooleanCellValue());
	              break;
	            case 5:
	              text.append(ErrorEval.getText(cell.getErrorCellValue()));
	              break;
	            case 2:
	              if (!this._shouldEvaluateFormulas)
	                text.append(cell.getCellFormula());
	              else {
	                switch (cell.getCachedFormulaResultType()) {
	                case 1:
	                  HSSFRichTextString str = cell.getRichStringCellValue();
	                  if ((str == null) || (str.length() <= 0)) break;
	                  text.append(str.toString()); break;
	                case 0:
	                  HSSFCellStyle style = cell.getCellStyle();
	                  if (style == null)
	                    text.append(cell.getNumericCellValue());
	                  else {
	                    text.append(this._formatter.formatRawCellContents(cell.getNumericCellValue(), style.getDataFormat(), style.getDataFormatString()));
	                  }

	                  break;
	                case 4:
	                  text.append(cell.getBooleanCellValue());
	                  break;
	                case 5:
	                  text.append(ErrorEval.getText(cell.getErrorCellValue()));
	                case 2:
	                case 3:
	                }
	              }
	              break;
	            case 3:
	            default:
	              throw new RuntimeException("Unexpected cell type (" + cell.getCellType() + ")");
	            }

	            HSSFComment comment = cell.getCellComment();
	            if ((this._includeCellComments) && (comment != null))
	            {
	              String commentText = comment.getString().getString().replace('\n', ' ');
	              text.append(" Comment by " + comment.getAuthor() + ": " + commentText);
	            }

	          }

	          if ((outputContents) && (k < lastCell - 1)) {
	            text.append("\t");
	          }

	        }

	        //text.append("\n");
	        count ++;
	        //text.append("\n");
	       // System.out.println(count + ":" + text.toString());
	        excelList.add(text.toString());
	      }
	      text = new StringBuffer();
	      if (this._includeHeadersFooters) {
	        text.append(_extractHeaderFooter(sheet.getFooter()));
	      }
	      if(StringUtils.isNotNull(text)){
	    	  excelList.add(text.toString());
	      }
	    }

	    return excelList;
  
  }

    /**
     * 返回文件总行数
     *
     */
    public int getLastRow(){
        this._wb.setMissingCellPolicy(HSSFRow.RETURN_BLANK_AS_NULL);
        for (int i = 0; i < this._wb.getNumberOfSheets(); i++) {
            HSSFSheet sheet = this._wb.getSheetAt(i);
            if (sheet == null)
                continue;
            int firstRow = sheet.getFirstRowNum();
            int lastRow = sheet.getLastRowNum();
            return lastRow;
        }
        return 0;
    }
    /**
     * 获取指定行间的数据
     * @return
     */
    public List<String> getList(int firstRowNo,int lastRowNo){
        List<String> excelList = new ArrayList<String>();
        StringBuffer text = new StringBuffer();
        this._wb.setMissingCellPolicy(HSSFRow.RETURN_BLANK_AS_NULL);
        for (int i = 0; i < this._wb.getNumberOfSheets(); i++) {
            HSSFSheet sheet = this._wb.getSheetAt(i);
            if (sheet == null)
                continue;
            if (this._includeSheetNames) {
                String name = this._wb.getSheetName(i);
                if (name != null) {
                    text.append(name);
                    text.append("\n");
                }

            }

            if (this._includeHeadersFooters) {
                text.append(_extractHeaderFooter(sheet.getHeader()));
            }

            if(StringUtils.isNotNull(text)){
                excelList.add(text.toString());
            }

            int firstRow = sheet.getFirstRowNum();
            int lastRow = sheet.getLastRowNum();
            int count = 0;
            for (int j = firstRow; j <= lastRow; j++) {
                HSSFRow row = sheet.getRow(j);
                if(count<firstRowNo){
                    continue;
                }
                if(count>lastRowNo){
                    break;
                }
                count++;
                if (row == null) {
                    continue;
                }

                int firstCell = row.getFirstCellNum();
                int lastCell = row.getLastCellNum();
                if (this._includeBlankCells) {
                    firstCell = 0;
                }
                text = new StringBuffer();
                for (int k = firstCell; k < lastCell; k++) {
                    HSSFCell cell = row.getCell(k);
                    boolean outputContents = true;

                    if (cell == null)
                    {
                        outputContents = this._includeBlankCells;
                    } else {
                        switch (cell.getCellType()) {
                            case 1:
                                text.append(cell.getRichStringCellValue().getString());
                                break;
                            case 0:
                                text.append(this._formatter.formatCellValue(cell));

                                break;
                            case 4:
                                text.append(cell.getBooleanCellValue());
                                break;
                            case 5:
                                text.append(ErrorEval.getText(cell.getErrorCellValue()));
                                break;
                            case 2:
                                if (!this._shouldEvaluateFormulas)
                                    text.append(cell.getCellFormula());
                                else {
                                    switch (cell.getCachedFormulaResultType()) {
                                        case 1:
                                            HSSFRichTextString str = cell.getRichStringCellValue();
                                            if ((str == null) || (str.length() <= 0)) break;
                                            text.append(str.toString()); break;
                                        case 0:
                                            HSSFCellStyle style = cell.getCellStyle();
                                            if (style == null)
                                                text.append(cell.getNumericCellValue());
                                            else {
                                                text.append(this._formatter.formatRawCellContents(cell.getNumericCellValue(), style.getDataFormat(), style.getDataFormatString()));
                                            }

                                            break;
                                        case 4:
                                            text.append(cell.getBooleanCellValue());
                                            break;
                                        case 5:
                                            text.append(ErrorEval.getText(cell.getErrorCellValue()));
                                        case 2:
                                        case 3:
                                    }
                                }
                                break;
                            case 3:
                            default:
                                throw new RuntimeException("Unexpected cell type (" + cell.getCellType() + ")");
                        }
                        HSSFComment comment = cell.getCellComment();
                        if ((this._includeCellComments) && (comment != null))
                        {
                            String commentText = comment.getString().getString().replace('\n', ' ');
                            text.append(" Comment by " + comment.getAuthor() + ": " + commentText);
                        }
                    }
                    if ((outputContents) && (k < lastCell - 1)) {
                        text.append("\t");
                    }
                }
                //text.append("\n");
                count ++;
                //text.append("\n");
                // System.out.println(count + ":" + text.toString());
                excelList.add(text.toString());
            }
            text = new StringBuffer();
            if (this._includeHeadersFooters) {
                text.append(_extractHeaderFooter(sheet.getFooter()));
            }
            if(StringUtils.isNotNull(text)){
                excelList.add(text.toString());
            }
        }
        return excelList;
    }

    /**
     * 获取指定行数的数据
     * @return
     */
    public List<String> getList(int No){
        List<String> excelList = new ArrayList<String>();
        StringBuffer text = new StringBuffer();
        this._wb.setMissingCellPolicy(HSSFRow.RETURN_BLANK_AS_NULL);
        for (int i = 0; i < this._wb.getNumberOfSheets(); i++) {
            HSSFSheet sheet = this._wb.getSheetAt(i);
            if (sheet == null)
                continue;
            if (this._includeSheetNames) {
                String name = this._wb.getSheetName(i);
                if (name != null) {
                    text.append(name);
                    text.append("\n");
                }

            }

            if (this._includeHeadersFooters) {
                text.append(_extractHeaderFooter(sheet.getHeader()));
            }

            if(StringUtils.isNotNull(text)){
                excelList.add(text.toString());
            }

            int firstRow = sheet.getFirstRowNum();
            int lastRow = sheet.getLastRowNum();
            int count = 0;
            for (int j = firstRow; j <= lastRow; j++) {
                HSSFRow row = sheet.getRow(j);
                if(j!=No){
                    continue;
                }
                if(j==No){
                    break;
                }
                if (row == null) {
                    continue;
                }

                int firstCell = row.getFirstCellNum();
                int lastCell = row.getLastCellNum();
                if (this._includeBlankCells) {
                    firstCell = 0;
                }
                text = new StringBuffer();
                for (int k = firstCell; k < lastCell; k++) {
                    HSSFCell cell = row.getCell(k);
                    boolean outputContents = true;

                    if (cell == null)
                    {
                        outputContents = this._includeBlankCells;
                    } else {
                        switch (cell.getCellType()) {
                            case 1:
                                text.append(cell.getRichStringCellValue().getString());
                                break;
                            case 0:
                                text.append(this._formatter.formatCellValue(cell));

                                break;
                            case 4:
                                text.append(cell.getBooleanCellValue());
                                break;
                            case 5:
                                text.append(ErrorEval.getText(cell.getErrorCellValue()));
                                break;
                            case 2:
                                if (!this._shouldEvaluateFormulas)
                                    text.append(cell.getCellFormula());
                                else {
                                    switch (cell.getCachedFormulaResultType()) {
                                        case 1:
                                            HSSFRichTextString str = cell.getRichStringCellValue();
                                            if ((str == null) || (str.length() <= 0)) break;
                                            text.append(str.toString()); break;
                                        case 0:
                                            HSSFCellStyle style = cell.getCellStyle();
                                            if (style == null)
                                                text.append(cell.getNumericCellValue());
                                            else {
                                                text.append(this._formatter.formatRawCellContents(cell.getNumericCellValue(), style.getDataFormat(), style.getDataFormatString()));
                                            }

                                            break;
                                        case 4:
                                            text.append(cell.getBooleanCellValue());
                                            break;
                                        case 5:
                                            text.append(ErrorEval.getText(cell.getErrorCellValue()));
                                        case 2:
                                        case 3:
                                    }
                                }
                                break;
                            case 3:
                            default:
                                throw new RuntimeException("Unexpected cell type (" + cell.getCellType() + ")");
                        }
                        HSSFComment comment = cell.getCellComment();
                        if ((this._includeCellComments) && (comment != null))
                        {
                            String commentText = comment.getString().getString().replace('\n', ' ');
                            text.append(" Comment by " + comment.getAuthor() + ": " + commentText);
                        }
                    }
                    if ((outputContents) && (k < lastCell - 1)) {
                        text.append("\t");
                    }
                }
                //text.append("\n");
                count ++;
                //text.append("\n");
                // System.out.println(count + ":" + text.toString());
                excelList.add(text.toString());
            }
            text = new StringBuffer();
            if (this._includeHeadersFooters) {
                text.append(_extractHeaderFooter(sheet.getFooter()));
            }
            if(StringUtils.isNotNull(text)){
                excelList.add(text.toString());
            }
        }
        return excelList;
    }

    /**
     * 读取指定行数的文件内容
     * @param No
     * @return
     */
    public String getText(int No){
        StringBuffer text = new StringBuffer();

        this._wb.setMissingCellPolicy(HSSFRow.RETURN_BLANK_AS_NULL);

        for (int i = 0; i < this._wb.getNumberOfSheets(); i++) {
            HSSFSheet sheet = this._wb.getSheetAt(i);
            if (sheet == null)
                continue;
            if (this._includeSheetNames) {
                String name = this._wb.getSheetName(i);
                if (name != null) {
                    text.append(name);
                    text.append("\n");
                }

            }

            if (this._includeHeadersFooters) {
                text.append(_extractHeaderFooter(sheet.getHeader()));
            }

            int firstRow = sheet.getFirstRowNum();
            int lastRow = sheet.getLastRowNum();
            for (int j = firstRow; j <= lastRow; j++) {
                HSSFRow row = sheet.getRow(j);
                if(j>No){
                    break;
                }
                if (row == null) {
                    continue;
                }
                int firstCell = row.getFirstCellNum();
                int lastCell = row.getLastCellNum();
                if (this._includeBlankCells) {
                    firstCell = 0;
                }

                for (int k = firstCell; k < lastCell; k++) {
                    HSSFCell cell = row.getCell(k);
                    boolean outputContents = true;

                    if (cell == null)
                    {
                        outputContents = this._includeBlankCells;
                    } else {
                        switch (cell.getCellType()) {
                            case 1:
                                text.append(cell.getRichStringCellValue().getString());
                                break;
                            case 0:
                                text.append(this._formatter.formatCellValue(cell));

                                break;
                            case 4:
                                text.append(cell.getBooleanCellValue());
                                break;
                            case 5:
                                text.append(ErrorEval.getText(cell.getErrorCellValue()));
                                break;
                            case 2:
                                if (!this._shouldEvaluateFormulas)
                                    text.append(cell.getCellFormula());
                                else {
                                    switch (cell.getCachedFormulaResultType()) {
                                        case 1:
                                            HSSFRichTextString str = cell.getRichStringCellValue();
                                            if ((str == null) || (str.length() <= 0)) break;
                                            text.append(str.toString()); break;
                                        case 0:
                                            HSSFCellStyle style = cell.getCellStyle();
                                            if (style == null)
                                                text.append(cell.getNumericCellValue());
                                            else {
                                                text.append(this._formatter.formatRawCellContents(cell.getNumericCellValue(), style.getDataFormat(), style.getDataFormatString()));
                                            }

                                            break;
                                        case 4:
                                            text.append(cell.getBooleanCellValue());
                                            break;
                                        case 5:
                                            text.append(ErrorEval.getText(cell.getErrorCellValue()));
                                        case 2:
                                        case 3:
                                    }
                                }
                                break;
                            case 3:
                            default:
                                throw new RuntimeException("Unexpected cell type (" + cell.getCellType() + ")");
                        }

                        HSSFComment comment = cell.getCellComment();
                        if ((this._includeCellComments) && (comment != null))
                        {
                            String commentText = comment.getString().getString().replace('\n', ' ');
                            text.append(" Comment by " + comment.getAuthor() + ": " + commentText);
                        }

                    }

                    if ((outputContents) && (k < lastCell - 1)) {
                        text.append("\t");
                    }

                }

                text.append("\n");
            }

            if (this._includeHeadersFooters) {
                text.append(_extractHeaderFooter(sheet.getFooter()));
            }
        }

        return text.toString();
    }
  public String getText()
  {
    StringBuffer text = new StringBuffer();

    this._wb.setMissingCellPolicy(HSSFRow.RETURN_BLANK_AS_NULL);

    for (int i = 0; i < this._wb.getNumberOfSheets(); i++) {
      HSSFSheet sheet = this._wb.getSheetAt(i);
      if (sheet == null)
        continue;
      if (this._includeSheetNames) {
        String name = this._wb.getSheetName(i);
        if (name != null) {
          text.append(name);
          text.append("\n");
        }

      }

      if (this._includeHeadersFooters) {
        text.append(_extractHeaderFooter(sheet.getHeader()));
      }

      int firstRow = sheet.getFirstRowNum();
      int lastRow = sheet.getLastRowNum();
      for (int j = firstRow; j <= lastRow; j++) {
        HSSFRow row = sheet.getRow(j);
        if (row == null) {
          continue;
        }
        int firstCell = row.getFirstCellNum();
        int lastCell = row.getLastCellNum();
        if (this._includeBlankCells) {
          firstCell = 0;
        }

        for (int k = firstCell; k < lastCell; k++) {
          HSSFCell cell = row.getCell(k);
          boolean outputContents = true;

          if (cell == null)
          {
            outputContents = this._includeBlankCells;
          } else {
            switch (cell.getCellType()) {
            case 1:
              text.append(cell.getRichStringCellValue().getString());
              break;
            case 0:
              text.append(this._formatter.formatCellValue(cell));

              break;
            case 4:
              text.append(cell.getBooleanCellValue());
              break;
            case 5:
              text.append(ErrorEval.getText(cell.getErrorCellValue()));
              break;
            case 2:
              if (!this._shouldEvaluateFormulas)
                text.append(cell.getCellFormula());
              else {
                switch (cell.getCachedFormulaResultType()) {
                case 1:
                  HSSFRichTextString str = cell.getRichStringCellValue();
                  if ((str == null) || (str.length() <= 0)) break;
                  text.append(str.toString()); break;
                case 0:
                  HSSFCellStyle style = cell.getCellStyle();
                  if (style == null)
                    text.append(cell.getNumericCellValue());
                  else {
                    text.append(this._formatter.formatRawCellContents(cell.getNumericCellValue(), style.getDataFormat(), style.getDataFormatString()));
                  }

                  break;
                case 4:
                  text.append(cell.getBooleanCellValue());
                  break;
                case 5:
                  text.append(ErrorEval.getText(cell.getErrorCellValue()));
                case 2:
                case 3:
                }
              }
              break;
            case 3:
            default:
              throw new RuntimeException("Unexpected cell type (" + cell.getCellType() + ")");
            }

            HSSFComment comment = cell.getCellComment();
            if ((this._includeCellComments) && (comment != null))
            {
              String commentText = comment.getString().getString().replace('\n', ' ');
              text.append(" Comment by " + comment.getAuthor() + ": " + commentText);
            }

          }

          if ((outputContents) && (k < lastCell - 1)) {
            text.append("\t");
          }

        }

        text.append("\n");
      }

      if (this._includeHeadersFooters) {
        text.append(_extractHeaderFooter(sheet.getFooter()));
      }
    }

    return text.toString();
  }

  public static String _extractHeaderFooter(HeaderFooter hf) {
    StringBuffer text = new StringBuffer();

    if (hf.getLeft() != null) {
      text.append(hf.getLeft());
    }
    if (hf.getCenter() != null) {
      if (text.length() > 0)
        text.append("\t");
      text.append(hf.getCenter());
    }
    if (hf.getRight() != null) {
      if (text.length() > 0)
        text.append("\t");
      text.append(hf.getRight());
    }
    if (text.length() > 0) {
      text.append("\n");
    }
    return text.toString();
  }

  private static final class CommandArgs
  {
    private final boolean _requestHelp;
    private final File _inputFile;
    private final boolean _showSheetNames;
    private final boolean _evaluateFormulas;
    private final boolean _showCellComments;
    private final boolean _showBlankCells;
    private final boolean _headersFooters;

    public CommandArgs(String[] args)
      throws ExcelExtractor.CommandParseException
    {
      int nArgs = args.length;
      File inputFile = null;
      boolean requestHelp = false;
      boolean showSheetNames = true;
      boolean evaluateFormulas = true;
      boolean showCellComments = false;
      boolean showBlankCells = false;
      boolean headersFooters = true;
      for (int i = 0; i < nArgs; i++) {
        String arg = args[i];
        if ("-help".equalsIgnoreCase(arg)) {
          requestHelp = true;
          break;
        }
        if ("-i".equals(arg))
        {
          i++; if (i >= nArgs) {
            throw new ExcelExtractor.CommandParseException("Expected filename after '-i'");
          }
          arg = args[i];
          if (inputFile != null) {
            throw new ExcelExtractor.CommandParseException("Only one input file can be supplied");
          }
          inputFile = new File(arg);
          if (!inputFile.exists()) {
            throw new ExcelExtractor.CommandParseException("Specified input file '" + arg + "' does not exist");
          }
          if (inputFile.isDirectory()) {
            throw new ExcelExtractor.CommandParseException("Specified input file '" + arg + "' is a directory");
          }

        }
        else if ("--show-sheet-names".equals(arg)) {
          i++; showSheetNames = parseBoolArg(args, i);
        }
        else if ("--evaluate-formulas".equals(arg)) {
          i++; evaluateFormulas = parseBoolArg(args, i);
        }
        else if ("--show-comments".equals(arg)) {
          i++; showCellComments = parseBoolArg(args, i);
        }
        else if ("--show-blanks".equals(arg)) {
          i++; showBlankCells = parseBoolArg(args, i);
        }
        else if ("--headers-footers".equals(arg)) {
          i++; headersFooters = parseBoolArg(args, i);
        }
        else {
          throw new ExcelExtractor.CommandParseException("Invalid argument '" + arg + "'");
        }
      }
      this._requestHelp = requestHelp;
      this._inputFile = inputFile;
      this._showSheetNames = showSheetNames;
      this._evaluateFormulas = evaluateFormulas;
      this._showCellComments = showCellComments;
      this._showBlankCells = showBlankCells;
      this._headersFooters = headersFooters;
    }
    private static boolean parseBoolArg(String[] args, int i) throws ExcelExtractor.CommandParseException {
      if (i >= args.length) {
        throw new ExcelExtractor.CommandParseException("Expected value after '" + args[(i - 1)] + "'");
      }
      String value = args[i].toUpperCase();
      if (("Y".equals(value)) || ("YES".equals(value)) || ("ON".equals(value)) || ("TRUE".equals(value))) {
        return true;
      }
      if (("N".equals(value)) || ("NO".equals(value)) || ("OFF".equals(value)) || ("FALSE".equals(value))) {
        return false;
      }
      throw new ExcelExtractor.CommandParseException("Invalid value '" + args[i] + "' for '" + args[(i - 1)] + "'. Expected 'Y' or 'N'");
    }
    public boolean isRequestHelp() {
      return this._requestHelp;
    }
    public File getInputFile() {
      return this._inputFile;
    }
    public boolean shouldShowSheetNames() {
      return this._showSheetNames;
    }
    public boolean shouldEvaluateFormulas() {
      return this._evaluateFormulas;
    }
    public boolean shouldShowCellComments() {
      return this._showCellComments;
    }
    public boolean shouldShowBlankCells() {
      return this._showBlankCells;
    }
    public boolean shouldIncludeHeadersFooters() {
      return this._headersFooters;
    }
  }

  private static final class CommandParseException extends Exception
  {
    public CommandParseException(String msg)
    {
      super();
    }
  }
}