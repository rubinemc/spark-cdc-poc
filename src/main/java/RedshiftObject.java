import java.util.Date;

public class RedshiftObject {

	private String sourceType ;
	private String schemaSystemName ;
	private Long objectId ;
	private String objectName;
	private String objectFields;
	private String objectDataType;
	private Boolean isPk ;
	private Boolean isCdc;
	private Boolean isMandatory ;
	private String fileDelimiter;
	private Boolean isFileHeader;
	private Boolean activeFlag;
	private Date startDate;
	private Date endDate;
	private String comments;
	private Integer objectFieldsOrder;
	private String filePath;
	
	
	public String getSourceType() {
		return sourceType;
	}
	public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}
	public String getSchemaSystemName() {
		return schemaSystemName;
	}
	public void setSchemaSystemName(String schemaSystemName) {
		this.schemaSystemName = schemaSystemName;
	}
	public Long getObjectId() {
		return objectId;
	}
	public void setObjectId(Long objectId) {
		this.objectId = objectId;
	}
	public String getObjectName() {
		return objectName;
	}
	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}
	public String getObjectFields() {
		return objectFields;
	}
	public void setObjectFields(String objectFields) {
		this.objectFields = objectFields;
	}
	public String getObjectDataType() {
		return objectDataType;
	}
	public void setObjectDataType(String objectDataType) {
		this.objectDataType = objectDataType;
	}
	public Boolean getIsPk() {
		return isPk;
	}
	public void setIsPk(Boolean isPk) {
		this.isPk = isPk;
	}
	public Boolean getIsCdc() {
		return isCdc;
	}
	public void setIsCdc(Boolean isCdc) {
		this.isCdc = isCdc;
	}
	public Boolean getIsMandatory() {
		return isMandatory;
	}
	public void setIsMandatory(Boolean isMandatory) {
		this.isMandatory = isMandatory;
	}
	public String getFileDelimiter() {
		return fileDelimiter;
	}
	public void setFileDelimiter(String fileDelimiter) {
		this.fileDelimiter = fileDelimiter;
	}
	public Boolean getIsFileHeader() {
		return isFileHeader;
	}
	public void setIsFileHeader(Boolean isFileHeader) {
		this.isFileHeader = isFileHeader;
	}
	public Boolean getActiveFlag() {
		return activeFlag;
	}
	public void setActiveFlag(Boolean activeFlag) {
		this.activeFlag = activeFlag;
	}
	public Date getStartDate() {
		return startDate;
	}
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	public Date getEndDate() {
		return endDate;
	}
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	public String getComments() {
		return comments;
	}
	public void setComments(String comments) {
		this.comments = comments;
	}
	public Integer getObjectFieldsOrder() {
		return objectFieldsOrder;
	}
	public void setObjectFieldsOrder(Integer objectFieldsOrder) {
		this.objectFieldsOrder = objectFieldsOrder;
	}
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	@Override
	public String toString() {
		return "RedshiftObject [sourceType=" + sourceType + ", schemaSystemName=" + schemaSystemName + ", objectId="
				+ objectId + ", objectName=" + objectName + ", objectFields=" + objectFields + ", objectDataType="
				+ objectDataType + ", isPk=" + isPk + ", isCdc=" + isCdc + ", isMandatory=" + isMandatory
				+ ", fileDelimiter=" + fileDelimiter + ", isFileHeader=" + isFileHeader + ", activeFlag=" + activeFlag
				+ ", startDate=" + startDate + ", endDate=" + endDate + ", comments=" + comments
				+ ", objectFieldsOrder=" + objectFieldsOrder + ", filePath=" + filePath + "]";
	}

}
