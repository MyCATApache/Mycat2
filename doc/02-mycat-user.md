

## mycat 2.0-user(user.yml,用户与安全相关配置)

author:junwen 2019-6-2

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

## 配置

```yaml
users:
  - name: root
    password: 123456
    schemas:
      - test
```

### name

使用mysql客户端登录mycat proxy的用户名

### password

上述用户名的密码

### schemas

允许访问的schema,引用schema配置的名字,以,分隔配置多个

------

