package com.hxqh.bigdata.spark.session;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 品类二次排序key
 * <p>
 * 封装排序算法需要的几个字段：点击次数、下单次数和支付次数
 * 实现Ordered接口要求的几个方法
 * <p>
 * 跟其他key相比，如何来判定大于、大于等于、小于、小于等于
 * <p>
 * 依次使用三个次数进行比较，如果某一个相等，那么就比较下一个
 * <p>
 * <p>
 * <p>
 * <p>
 * Created by Ocean lin on 2018/1/8.
 *
 * @author Ocean Lin
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

    private long clickCount;
    private long orderCount;
    private long payCount;

    public CategorySortKey() {
    }

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public int compare(CategorySortKey that) {
        if (clickCount - that.getClickCount() != 0) {
            return (int) (clickCount - that.getClickCount());
        } else if (orderCount - that.getOrderCount() != 0) {
            return (int) (orderCount - that.getOrderCount());
        } else if (payCount - that.getPayCount() != 0) {
            return (int) (payCount - that.getPayCount());
        }
        return 0;
    }

    @Override
    public boolean $less(CategorySortKey other) {
        if (clickCount < other.getClickCount()) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount < other.getOrderCount()) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount < other.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey other) {
        if (clickCount > other.getClickCount()) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount > other.getOrderCount()) {
            return true;
        } else if (clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount > other.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey that) {
        if ($less(that)) {
            return true;
        } else if (clickCount == that.getClickCount() &&
                orderCount == that.getOrderCount() &&
                payCount == that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey that) {
        if ($greater(that)) {
            return true;
        } else if (clickCount == that.getClickCount() &&
                orderCount == that.getOrderCount() &&
                payCount == that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(CategorySortKey that) {
        if (clickCount - that.getClickCount() != 0) {
            return (int) (clickCount - that.getClickCount());
        } else if (orderCount - that.getOrderCount() != 0) {
            return (int) (orderCount - that.getOrderCount());
        } else if (payCount - that.getPayCount() != 0) {
            return (int) (payCount - that.getPayCount());
        }
        return 0;
    }


    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }
}
