package org.example.test;

public interface test2<T> extends testInterface{
    static <T> test2<T> xxx() {
        // 返回一个 testInterface 的实现
        return (String x) -> new test3<>();
    }
}
