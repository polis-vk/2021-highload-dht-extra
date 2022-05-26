# 2021-highload-dht-extra
Задание для пересдачи [курса](https://polis.mail.ru/curriculum/program/discipline/1257/) "Проектирование высоконагруженных систем" в [Технополис](https://polis.mail.ru).

## TBD
### Fork
[Форкните проект](https://help.github.com/articles/fork-a-repo/), склонируйте и добавьте `upstream`:
```
$ git clone git@github.com:<username>/2021-highload-dht-extra.git
Cloning into '2021-highload-dht-extra'...
...
$ git remote add upstream git@github.com:polis-mail-ru/2021-highload-dht-extra.git
$ git fetch upstream
From github.com:polis-mail-ru/2021-highload-dht-extra
 * [new branch]      main     -> upstream/main
```

### Make
Так можно запустить тесты:
```
$ ./gradlew test
```

### Develop
Откройте в IDE -- [IntelliJ IDEA Community Edition](https://www.jetbrains.com/idea/) нам будет достаточно.

Реализуйте метод [`RepairingMerger.mergeAndRepair()`](src/main/java/ru/mail/polis/RepairingMerger.java),
руководствуясь JavaDoc, модульными тестами и пояснениями преподавателя.

**ВНИМАНИЕ!** При запуске тестов в IDE необходимо передавать Java опцию `-Xmx64m`, потому что
именно с таким размером хипа будут прогоняться тесты на агентах.

### Report
Когда решение будет готово, присылайте pull request со своей реализацией на review.

### Testing
В процессе экзамена будут публиковаться новые наборы тестов -- подмёрдживайте тесты, исправляйте баги и
отвечайте на замечания ревьюера.