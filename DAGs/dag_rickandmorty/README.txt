> �������
�������� � GreenPlum'� ������� � ��������� "<���_�����>_ram_location" � ������ id, name, type, dimension, resident_cnt.
! �������� ��������: ��� ����� � LMS ����� ������������, ������� ����� �� ������ �������������
� ������� API (https://rickandmortyapi.com/documentation/#location) ������� ��� ������� ������� "��� � �����" � ���������� ����������� ����������.
�������� �������� ��������������� ����� ���� ��� ������� � �������. resident_cnt � ����� ������ � ���� residents.
> hint
* ��� ������ � GreenPlum ������������ ���������� 'conn_greenplum_write' � ������, ���� �� ��������� � LMS ���� ��������� ���������� �������������� � ����� ������ Airflow. ��������� ����������:

Host: greenplum.lab.karpov.courses
Port: 6432
DataBase: students (�� karpovcourses!!!)
Login: student
Password: Wrhy96_09iPcreqAS

* �� ��������, ��������� ���� ����� � LMS, �������� ����� �� ������ �������������

* ����� ������������ ��� PostgresHook, ����� �������� PostgresOperator

* ��������������� ������������ ���������� ���� �������� ��� ���������� top-3 ������� �� API

* ����� ������������ XCom ��� �������� �������� ����� �������, ����� ����� ���������� ������ �������� � �������

* �� �������� ���������� ��������� ������ ������� �����: ������������� ��������� �������� �������, ������������ �� ���������� � ��� ������

pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
pg_hook.run(sql_statement, False)


 