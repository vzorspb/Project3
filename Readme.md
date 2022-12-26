Итоговая аттестация по курсу "Инженер данных"

Проект № 3.

Анализ логов

Общая задача: создать скрипт для формирования витрины на основе логов web-сайта.

Подробное описание задачи:

Разработать скрипт формирования витрины следующего содержания:
1.      Суррогатный ключ устройства
2.      Название устройства
3.      Количество пользователей
4.      Доля пользователей данного устройства от общего числа пользователей.
5.      Количество совершенных действий для данного устройства
6.      Доля совершенных действий с данного устройства, относительно других устройств
7.      Список из 5 самых популярных браузеров, используемых на данном устройстве различными пользователями, с указанием доли использования для данного браузера относительно остальных браузеров. 
8.      Количество ответов сервера отличных от 200 на данном устройстве
9.      Для каждого из ответов сервера, отличных от 200, сформировать поле, в котором будет содержаться количество ответов данного типа

Источники:

https://disk.yandex.ru/d/BsdiH3DMTHpPrw 

Предварительный анализ данных:
в полученном для обработки архиве содержатся 2 файла:
    итого 3,3G
    3,3G access.log   
    13M client_hostname.csv
Первый содержит логи WEB сервера за период с 22.01.2019 3:56 по 26.01.2019 20:29 объемом 3.3G
Второй список IP адресов клиентов с информацией о наличии обратной зоны на DNS сервере. (использовать не будем, т.к. в файле отсутствуют данные, необходимые для решения задачи)

Лог файл содержит данные за 5 дней 16 часов 33 минуты = 8193 мин.
Исходя из того, что в году у нас 525600 минуть, делаем предположение, что логи за год у нас заямут 211,7 Гб.

Попытка № 1 (неудачная)
   Для  решения задачи выберем следующий технологический стек: Python + PostgreSQL.
   Для парсинга user-agent будем использовать библиотеку https://pypi.org/project/user-agents/
   Запрос данных с групировкой по одному полю занимал более 15 минут.
   Дождаться завершения запроса, содержащего JOIN не удалось.
   Попытку признаем неудачной и меняем базу данных.
   
Попытка № 2 (удачная)
   Для  решения задачи выберем следующий технологический стек: Python + ClickHouse.
   Запрос данных с групировкой по одному полю занимал более 15 минут.
   
   Проект содержит 2 скрипта:
   1. Создает таблицу в базе данных ClickHouse и загружает туда Log дополняя нанными парсинга поля useragent
      Запись в базу данных производится блаками по 100000 строк.
      Скрипт расположен:
      /src/clickhouse.py
   2. Формирует витрину в виде текстового файла в соответствии с заданием
      Скрипт расположен:
      /src/generate.py
      
      При переходе на ClickHouse все данные сведены в одну плоскую таблицу (в целях оптимизации скорости работы)
      Время формирования витрины на обычном компе на базе i5/8Gb/SSD составляет порядка 10 секунд.

      Используется плаская таблица следующей структуры:
      ip String: IP адрес клиентв
      datettime String: время запроса
      method String: метод запроса (GET,POST, ...)
      path String: путь запрашиваемой у сервера страницы
      resultcode String: код ртвета сервера
      size Int32: количество отданных данных в байтах
      referer String: адрес сервера, отвуда к нам перешли по ссылке
      useragent String: строка useragent, отдаваемая браузером клиента при запросе
      osfamily String: Наименование ОС клиента
      osversion String: Версия ОС клиента
      devicefamily String: наименование устройства клиента
      devicebrand String: наименование производителя устройства
      devicemodel String: наименование модели устройства
      browserfamily String: наименование используемого браузера
      browserversion String: версия используемого браузера 
      ismobile Bool: признак мобильного устройства
      ispc Bool: признак стационарного компьютера
      isbot Bool: признак робота
      
      
Результат работы скриптов представлен ниже


Количество уникальных посетителей		: 307335 		Количество действий: 10364857
   из них:
-------------------------------------------------------------------------------------------------------------
     мобильные устройства			:   241576 ( 78.6035 %)		  4400812 ( 42.4590 %)
-------------------------------------------------------------------------------------------------------------
                    Android             	:  211148(68.7029%)  	 3738479(36.0688%)	Chrome Mobile/1769606/47%[('301', 3746), ('302', 53481), ('304', 186268), ('400', 2), ('403', 4152), ('404', 19794), ('408', 45), ('499', 20615), ('500', 71), ('502', 219), ('504', 24)]
												Samsung Internet/655179/17%[('301', 3746), ('302', 53481), ('304', 186268), ('400', 2), ('403', 4152), ('404', 19794), ('408', 45), ('499', 20615), ('500', 71), ('502', 219), ('504', 24)]
												Googlebot/450499/12%[('301', 3746), ('302', 53481), ('304', 186268), ('400', 2), ('403', 4152), ('404', 19794), ('408', 45), ('499', 20615), ('500', 71), ('502', 219), ('504', 24)]
												Android/361973/9%[('301', 3746), ('302', 53481), ('304', 186268), ('400', 2), ('403', 4152), ('404', 19794), ('408', 45), ('499', 20615), ('500', 71), ('502', 219), ('504', 24)]
												Firefox Mobile/162305/4%[('301', 3746), ('302', 53481), ('304', 186268), ('400', 2), ('403', 4152), ('404', 19794), ('408', 45), ('499', 20615), ('500', 71), ('502', 219), ('504', 24)]
                    iOS                 	:   30039(9.7740%)  	  650101(6.2722%)	Mobile Safari/438410/67%[('301', 962), ('302', 11051), ('304', 2127), ('400', 1), ('403', 316), ('404', 67541), ('408', 8), ('499', 3191), ('500', 9), ('502', 19)]
												Chrome Mobile iOS/73400/11%[('301', 962), ('302', 11051), ('304', 2127), ('400', 1), ('403', 316), ('404', 67541), ('408', 8), ('499', 3191), ('500', 9), ('502', 19)]
												MobileSafari/69822/10%[('301', 962), ('302', 11051), ('304', 2127), ('400', 1), ('403', 316), ('404', 67541), ('408', 8), ('499', 3191), ('500', 9), ('502', 19)]
												Google/43051/6%[('301', 962), ('302', 11051), ('304', 2127), ('400', 1), ('403', 316), ('404', 67541), ('408', 8), ('499', 3191), ('500', 9), ('502', 19)]
												torob/11183/1%[('301', 962), ('302', 11051), ('304', 2127), ('400', 1), ('403', 316), ('404', 67541), ('408', 8), ('499', 3191), ('500', 9), ('502', 19)]
                    Windows Phone       	:     175(0.0569%)  	    7470(0.0721%)	Edge Mobile/4688/62%[('302', 72), ('304', 13), ('403', 1), ('404', 292), ('499', 59)]
												IE Mobile/2782/37%[('302', 72), ('304', 13), ('403', 1), ('404', 292), ('499', 59)]
                    Other               	:     175(0.0569%)  	    3598(0.0347%)	Opera Mini/3325/92%[('301', 2), ('302', 11), ('304', 1126), ('400', 1), ('403', 2), ('404', 8), ('499', 9)]
												UC Browser/207/5%[('301', 2), ('302', 11), ('304', 1126), ('400', 1), ('403', 2), ('404', 8), ('499', 9)]
												Nokia Services (WAP) Browser/56/1%[('301', 2), ('302', 11), ('304', 1126), ('400', 1), ('403', 2), ('404', 8), ('499', 9)]
												NetFront/6/0%[('301', 2), ('302', 11), ('304', 1126), ('400', 1), ('403', 2), ('404', 8), ('499', 9)]
												libwww-perl/4/0%[('301', 2), ('302', 11), ('304', 1126), ('400', 1), ('403', 2), ('404', 8), ('499', 9)]
                    BlackBerry OS       	:      29(0.0094%)  	     543(0.0052%)	BlackBerry WebKit/543/100%[('301', 3), ('302', 4), ('304', 35), ('404', 21), ('499', 3)]
                    Windows             	:       6(0.0020%)  	     353(0.0034%)	IE/350/99%[('302', 4), ('404', 1), ('499', 1)]
												Chrome/3/0%[('302', 4), ('404', 1), ('499', 1)]
                    Linux               	:       2(0.0007%)  	     217(0.0021%)	Chrome Mobile WebView/216/99%[('302', 3)]
												Puffin/1/0%[('302', 3)]
                    Symbian OS          	:       1(0.0003%)  	      50(0.0005%)	Opera Mini/50/100%[('304', 14)]
                    Windows Mobile      	:       1(0.0003%)  	       1(0.0000%)	ZDM/1/100%[]
-------------------------------------------------------------------------------------------------------------
     стационарные устройства			:   51459 ( 16.7436 %)		  5254914 ( 50.6993 %)
-------------------------------------------------------------------------------------------------------------
                    Windows             	:   49183(16.0031%)  	 5113983(49.3396%)	Chrome/2700682/52%[('206', 3), ('301', 16406), ('302', 78631), ('304', 32059), ('400', 6), ('401', 307), ('403', 175), ('404', 5257), ('405', 6), ('408', 59), ('499', 24668), ('500', 14071), ('502', 344), ('504', 42)]
												Firefox/2080371/40%[('206', 3), ('301', 16406), ('302', 78631), ('304', 32059), ('400', 6), ('401', 307), ('403', 175), ('404', 5257), ('405', 6), ('408', 59), ('499', 24668), ('500', 14071), ('502', 344), ('504', 42)]
												IE/181256/3%[('206', 3), ('301', 16406), ('302', 78631), ('304', 32059), ('400', 6), ('401', 307), ('403', 175), ('404', 5257), ('405', 6), ('408', 59), ('499', 24668), ('500', 14071), ('502', 344), ('504', 42)]
												Opera/69518/1%[('206', 3), ('301', 16406), ('302', 78631), ('304', 32059), ('400', 6), ('401', 307), ('403', 175), ('404', 5257), ('405', 6), ('408', 59), ('499', 24668), ('500', 14071), ('502', 344), ('504', 42)]
												Edge/53142/1%[('206', 3), ('301', 16406), ('302', 78631), ('304', 32059), ('400', 6), ('401', 307), ('403', 175), ('404', 5257), ('405', 6), ('408', 59), ('499', 24668), ('500', 14071), ('502', 344), ('504', 42)]
                    Linux               	:    1294(0.4210%)  	   43389(0.4186%)	Chrome/37492/86%[('301', 626), ('302', 1069), ('304', 90), ('403', 4), ('404', 86), ('499', 106), ('502', 7)]
												Samsung Internet/2698/6%[('301', 626), ('302', 1069), ('304', 90), ('403', 4), ('404', 86), ('499', 106), ('502', 7)]
												Opera/1144/2%[('301', 626), ('302', 1069), ('304', 90), ('403', 4), ('404', 86), ('499', 106), ('502', 7)]
												Firefox/1126/2%[('301', 626), ('302', 1069), ('304', 90), ('403', 4), ('404', 86), ('499', 106), ('502', 7)]
												Chrome Mobile WebView/216/0%[('301', 626), ('302', 1069), ('304', 90), ('403', 4), ('404', 86), ('499', 106), ('502', 7)]
                    Mac OS X            	:     845(0.2749%)  	   90317(0.8714%)	Safari/37603/41%[('301', 900), ('302', 6902), ('304', 239), ('401', 16), ('403', 33), ('404', 874), ('499', 641), ('500', 83), ('502', 14)]
												Chrome/24164/26%[('301', 900), ('302', 6902), ('304', 239), ('401', 16), ('403', 33), ('404', 874), ('499', 641), ('500', 83), ('502', 14)]
												Applebot/21773/24%[('301', 900), ('302', 6902), ('304', 239), ('401', 16), ('403', 33), ('404', 874), ('499', 641), ('500', 83), ('502', 14)]
												Firefox/4766/5%[('301', 900), ('302', 6902), ('304', 239), ('401', 16), ('403', 33), ('404', 874), ('499', 641), ('500', 83), ('502', 14)]
												Opera/1057/1%[('301', 900), ('302', 6902), ('304', 239), ('401', 16), ('403', 33), ('404', 874), ('499', 641), ('500', 83), ('502', 14)]
                    Ubuntu              	:     124(0.0403%)  	    5294(0.0511%)	Firefox/4099/77%[('301', 154), ('302', 269), ('304', 4), ('404', 11), ('499', 18), ('500', 3)]
												Chromium/1136/21%[('301', 154), ('302', 269), ('304', 4), ('404', 11), ('499', 18), ('500', 3)]
												pingbot/54/1%[('301', 154), ('302', 269), ('304', 4), ('404', 11), ('499', 18), ('500', 3)]
												HeadlessChrome/5/0%[('301', 154), ('302', 269), ('304', 4), ('404', 11), ('499', 18), ('500', 3)]
                    Chrome OS           	:       6(0.0020%)  	     339(0.0033%)	Chrome/339/100%[]
                    Fedora              	:       6(0.0020%)  	    1545(0.0149%)	Firefox/1290/83%[('301', 1), ('302', 8), ('499', 8)]
												Chrome/255/16%[('301', 1), ('302', 8), ('499', 8)]
                    Mageia              	:       1(0.0003%)  	      47(0.0005%)	Firefox/47/100%[]
-------------------------------------------------------------------------------------------------------------
     роботы              			:   3881 ( 1.2628 %)		  1096219 ( 10.5763 %)
-------------------------------------------------------------------------------------------------------------
                    BingPreview         	:    1937(0.6303%)  	   14154(0.1366%)	BingPreview/14154/100%[('301', 53), ('302', 2)]
                    bingbot             	:     508(0.1653%)  	  200596(1.9353%)	bingbot/200596/100%[('301', 27176), ('302', 1445), ('404', 2225), ('499', 452), ('500', 4), ('502', 55)]
                    AhrefsBot           	:     430(0.1399%)  	   57167(0.5515%)	AhrefsBot/57167/100%[('301', 96), ('302', 9529), ('404', 147), ('499', 419), ('500', 2), ('502', 20)]
                    Baiduspider         	:     182(0.0592%)  	     405(0.0039%)	Baiduspider/405/100%[('301', 156), ('302', 11), ('404', 13)]
                    Googlebot           	:     178(0.0579%)  	  642526(6.1991%)	Googlebot/642526/100%[('301', 4864), ('302', 37519), ('304', 165270), ('404', 11209), ('499', 4), ('500', 38), ('502', 168), ('504', 34)]
                    WhatsApp            	:     152(0.0495%)  	     692(0.0067%)	WhatsApp/692/100%[('301', 2), ('302', 109), ('404', 1), ('499', 26)]
                    MJ12bot             	:      94(0.0306%)  	    8584(0.0828%)	MJ12bot/8584/100%[('301', 178), ('302', 810), ('404', 167), ('499', 3), ('502', 2)]
                    Googlebot-Image     	:      65(0.0211%)  	  159118(1.5352%)	Googlebot-Image/159118/100%[('301', 44), ('302', 42), ('304', 118190), ('404', 49), ('500', 1), ('502', 3), ('504', 3)]
                    FacebookBot         	:      59(0.0192%)  	     105(0.0010%)	FacebookBot/105/100%[('301', 1), ('302', 3), ('404', 1), ('499', 1)]
                    Other               	:      35(0.0114%)  	      47(0.0005%)	Other/47/100%[('301', 12), ('404', 4)]
                    Mail.RU_Bot         	:      31(0.0101%)  	     155(0.0015%)	Mail.RU_Bot/155/100%[('301', 31), ('302', 7), ('404', 16)]
                    YandexBot           	:      23(0.0075%)  	    1029(0.0099%)	YandexBot/1029/100%[('301', 34), ('403', 800)]
                    SeznamBot           	:      21(0.0068%)  	     118(0.0011%)	SeznamBot/118/100%[('301', 2)]
                    YandexImageResizer  	:      21(0.0068%)  	      28(0.0003%)	YandexImageResizer/28/100%[]
                    WordPress           	:      15(0.0049%)  	      48(0.0005%)	WordPress/48/100%[('301', 1), ('404', 29), ('499', 4)]
                    YahooMailProxy      	:      12(0.0039%)  	      52(0.0005%)	YahooMailProxy/52/100%[('301', 7)]
                    YandexImages        	:      12(0.0039%)  	     438(0.0042%)	YandexImages/438/100%[('301', 2)]
                    Parsijoobot         	:      10(0.0033%)  	    9253(0.0893%)	Parsijoobot/9253/100%[('301', 2638), ('302', 2817), ('404', 56), ('499', 31), ('502', 2)]
                    Java                	:       8(0.0026%)  	      20(0.0002%)	Java/20/100%[]
                    Qwantify            	:       8(0.0026%)  	      91(0.0009%)	Qwantify/91/100%[('301', 19), ('302', 2), ('404', 2)]
                    Chrome              	:       6(0.0020%)  	      53(0.0005%)	Chrome/53/100%[]
                    Datanyze            	:       6(0.0020%)  	      89(0.0009%)	Datanyze/89/100%[('301', 1), ('302', 4), ('404', 3)]
                    bitlybot            	:       6(0.0020%)  	      35(0.0003%)	bitlybot/35/100%[('499', 15)]
                    Sogou web spider    	:       5(0.0016%)  	      10(0.0001%)	Sogou web spider/10/100%[('301', 2), ('400', 5)]
                    Yahoo! Slurp        	:       5(0.0016%)  	      12(0.0001%)	Yahoo! Slurp/12/100%[('301', 3)]
                    AdsBot-Google       	:       5(0.0016%)  	      25(0.0002%)	AdsBot-Google/25/100%[('302', 4)]
                    robot               	:       5(0.0016%)  	      30(0.0003%)	robot/30/100%[('301', 9), ('302', 1), ('404', 3)]
                    heritrix            	:       4(0.0013%)  	      20(0.0002%)	heritrix/20/100%[]
                    AwarioRssBot        	:       3(0.0010%)  	      24(0.0002%)	AwarioRssBot/24/100%[]
                    Slack-ImgProxy      	:       3(0.0010%)  	       3(0.0000%)	Slack-ImgProxy/3/100%[]
                    DuckDuckGo-Favicons-Bot	:       2(0.0007%)  	       2(0.0000%)	DuckDuckGo-Favicons-Bot/2/100%[]
                    YisouSpider         	:       2(0.0007%)  	       3(0.0000%)	YisouSpider/3/100%[('301', 1)]
                    Twitterbot          	:       2(0.0007%)  	       5(0.0000%)	Twitterbot/5/100%[('301', 1)]
                    DotBot              	:       2(0.0007%)  	     557(0.0054%)	DotBot/557/100%[('301', 162), ('302', 2), ('404', 13), ('499', 1)]
                    Python-urllib       	:       2(0.0007%)  	       3(0.0000%)	Python-urllib/3/100%[('301', 1), ('403', 1)]
                    SemrushBot          	:       2(0.0007%)  	      12(0.0001%)	SemrushBot/12/100%[]
                    archive.org_bot     	:       2(0.0007%)  	       4(0.0000%)	archive.org_bot/4/100%[('301', 3)]
                    Cliqzbot            	:       2(0.0007%)  	       5(0.0000%)	Cliqzbot/5/100%[]
                    policy adbeat_bot   	:       2(0.0007%)  	       4(0.0000%)	policy adbeat_bot/4/100%[('301', 1)]
                    IndeedBot           	:       1(0.0003%)  	       4(0.0000%)	IndeedBot/4/100%[('301', 1), ('404', 1)]
                    SEMrushBot          	:       1(0.0003%)  	      75(0.0007%)	SEMrushBot/75/100%[('301', 27)]
                    ia_archiver         	:       1(0.0003%)  	     170(0.0016%)	ia_archiver/170/100%[('301', 170)]
                    Exabot              	:       1(0.0003%)  	     104(0.0010%)	Exabot/104/100%[('301', 6), ('302', 9), ('499', 1)]
                    UptimeRobot         	:       1(0.0003%)  	       5(0.0000%)	UptimeRobot/5/100%[]
                    crawler             	:       1(0.0003%)  	       3(0.0000%)	crawler/3/100%[('302', 1)]
                    SMTBot              	:       1(0.0003%)  	       1(0.0000%)	SMTBot/1/100%[]
                    Genieo              	:       1(0.0003%)  	       1(0.0000%)	Genieo/1/100%[('301', 1)]
                    CCBot               	:       1(0.0003%)  	     166(0.0016%)	CCBot/166/100%[('499', 1)]
                    OdklBot             	:       1(0.0003%)  	       1(0.0000%)	OdklBot/1/100%[]
                    YandexMobileBot     	:       1(0.0003%)  	       3(0.0000%)	YandexMobileBot/3/100%[('403', 3)]
                    crawl               	:       1(0.0003%)  	     160(0.0015%)	crawl/160/100%[('404', 1)]
                    spider              	:       1(0.0003%)  	       2(0.0000%)	spider/2/100%[('301', 1)]
                    Direct Bot          	:       1(0.0003%)  	       2(0.0000%)	Direct Bot/2/100%[('301', 1)]
