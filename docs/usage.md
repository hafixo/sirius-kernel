![sirius](sirius.jpg)

# Usage

SIRIUS can be obtained via our custom maven repository. To use this library in your own project include:
```
<repositories>
    <repository>
        <id>scireum-mvn</id>
        <url>https://mvn.scireum.com</url>
    </repository>
</repositories>
    
<dependency>
    <groupId>com.scireum</groupId>
    <artifactId>sirius-MODULE</artifactId>
    <version>PICK A VERSION</version>
</dependency>
```
An overview of all versions can be found in
[our Maven repository](https://mvn.scireum.com/com/scireum/). 
Releases are also available in **Maven Central**

When writing server software 
(using https://github.com/scireum/sirius-web or https://github.com/scireum/sirius-biz)
we highly recommend using a **Docker** deployment based on
https://github.com/scireum/docker-sirius-runtime

**Important Information:** The [Classpath](../src/main/java/sirius/kernel/Classpath.java), which
is responsible for discovering all classes and resources only scans classpath roots which contain
a file named **component.marker** at its top-level. Therefore each module and the application
itself must place such a file in the resources folder so that it ends up in the resulting JAR.
