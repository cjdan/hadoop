package Spark.test;

import java.io.Serializable;

public class WebSiteInfo implements Serializable, Comparable<WebSiteInfo>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String website;
	private String area;
	private Integer total;
	
	public WebSiteInfo(String website, String area, Integer total) {
		super();
		this.website = website;
		this.area = area;
		this.total = total;
	}

	public String getWebsite() {
		return website;
	}

	public void setWebsite(String website) {
		this.website = website;
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		this.area = area;
	}

	public Integer getTotal() {
		return total;
	}

	public void setTotal(Integer total) {
		this.total = total;
	}

	@Override
	public int compareTo(WebSiteInfo webSiteInfo) {
		if(webSiteInfo.website.hashCode()-this.website.hashCode()==0){
			//比第二和第三位
			if(webSiteInfo.area.hashCode()-this.area.hashCode()!=0){
				//比第三位
				if(webSiteInfo.total-this.total!=0){
					return webSiteInfo.total-this.total;
				}
			}
			return webSiteInfo.area.hashCode()-this.area.hashCode();
		}
		return webSiteInfo.website.hashCode()-this.website.hashCode();
	}

}
