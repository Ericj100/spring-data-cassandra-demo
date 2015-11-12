package com.certusnet.cassandra.spring_cassandra.domain;

import java.util.List;

public class Pager {

	/**
     * 第几页
     */
    private Integer pageIndex = 1;

    /**
     * 每页显示多少条
     */
    private Integer pageNum = 10;

    /**
     * 总记录条数
     */
    private Integer totalNum = 0;
    
    private Integer totalPage;
    
    /**
	 * 分页页码列表
	 * 例如: 
	 * [1,2,3,4,5,null,10] 其中null代表省略号...
	 */
	private List<Integer> pageItems;
	
	private int pageMargin = 2;

    public Pager() {
        super();
    }

    public Pager(Integer pageIndex, Integer pageNum) {
        super();
        setPageIndex(pageIndex);
        setPageNum(pageNum);
    }

	public Integer getPageIndex() {
		return pageIndex;
	}

	public void setPageIndex(Integer pageIndex) {
		this.pageIndex = pageIndex;
	}

	public Integer getPageNum() {
		return pageNum;
	}

	public void setPageNum(Integer pageNum) {
		this.pageNum = pageNum;
	}

	public Integer getTotalNum() {
		return totalNum;
	}

	public void setTotalNum(Integer totalNum) {
		this.totalNum = totalNum == null ? 0 : totalNum;
        if (this.totalNum > 0) {
            this.totalPage = this.totalNum % this.pageNum == 0 ? (this.totalNum / this.pageNum) : ((this.totalNum / this.pageNum) + 1);
        } else {
            this.totalPage = 0;
        }
	}

	public Integer getTotalPage() {
		return totalPage;
	}

	public void setTotalPage(Integer totalPage) {
		this.totalPage = totalPage;
	}

	public List<Integer> getPageItems() {
		return pageItems;
	}

	public void setPageItems(List<Integer> pageItems) {
		this.pageItems = pageItems;
	}

	public int getPageMargin() {
		return pageMargin;
	}

	public void setPageMargin(int pageMargin) {
		this.pageMargin = pageMargin;
	}
	
	public static Pager all(){
		Pager all = new Pager();
		all.setPageIndex(1);
		all.setPageNum(Integer.MAX_VALUE);
		return all;
	}
	
	
	public boolean isAll(){
		return this.getPageNum() == Integer.MAX_VALUE;
	}
}
