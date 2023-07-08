from sqlalchemy import func, desc, distinct

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_1():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    :return:
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_2(subject):
    """Знайти студента із найвищим середнім балом з певного предмета."""

    discipline = session.query(Discipline).filter(Discipline.name == subject).first()

    if discipline is None:
        print(f"Предмет '{subject}' не знайдено")
        return

    highest_avg_grade = session.query(Student, func.avg(Grade.grade).label('average_grade')) \
        .join(Grade).filter(Grade.discipline_id == discipline.id) \
        .group_by(Student.id) \
        .order_by(func.avg(Grade.grade).desc()) \
        .first()

    if highest_avg_grade is not None:
        student, avg_grade = highest_avg_grade
        print(f"Студент з найвищим середнім балом з предмета '{subject}':")
        print(f"ПІБ: {student.fullname}")
        print(f"Середній бал: {avg_grade}")
    else:
        print(f"Не вдалося знайти студента з найвищим середнім балом з предмета '{subject}'")


def select_3(discipline_id):
    """Знайти середній бал у групах з певного предмета."""

    result = session.query(Discipline.name, Group.name, func.avg(Grade.grade)) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Discipline.name, Group.name) \
        .all()

    return result


def select_4():
    """Знайти середній бал на потоці (по всій таблиці оцінок)."""

    average_grade = session.query(func.avg(Grade.grade)).scalar()

    if average_grade is not None:
        print(f"Середній бал на потоці: {average_grade}")
    else:
        print("Не вдалося обчислити середній бал на потоці")


def select_5(teacher_name):
    """Знайти які курси читає певний викладач."""

    teacher = session.query(Teacher).filter(Teacher.fullname == teacher_name).first()

    if teacher is None:
        print(f"Викладача '{teacher_name}' не знайдено")
        return

    courses = session.query(Discipline.name).join(Teacher).filter(Teacher.id == teacher.id).all()

    if courses:
        print(f"Курси, які читає викладач '{teacher_name}':")
        for course in courses:
            print(course[0])
    else:
        print(f"Не вдалося знайти курси, які читає викладач '{teacher_name}'")


def select_6(group_name):
    """Знайти список студентів у певній групі."""

    group = session.query(Group).filter(Group.name == group_name).first()

    if group is None:
        print(f"Групу '{group_name}' не знайдено")
        return

    students = session.query(Student.fullname).join(Group).filter(Group.id == group.id).all()

    if students:
        print(f"Студенти у групі '{group_name}':")
        for student in students:
            print(student[0])
    else:
        print(f"Не вдалося знайти студентів у групі '{group_name}'")


def select_7(group_name, discipline_name):
    """Знайти оцінки студентів у окремій групі з певного предмета."""

    group = session.query(Group).filter(Group.name == group_name).first()

    if group is None:
        print(f"Групу '{group_name}' не знайдено")
        return

    discipline = session.query(Discipline).filter(Discipline.name == discipline_name).first()

    if discipline is None:
        print(f"Предмет '{discipline_name}' не знайдено")
        return

    grades = (
        session.query(Grade)
        .join(Student)
        .join(Discipline)
        .filter(Student.group_id == group.id, Discipline.id == discipline.id)
        .all()
    )

    if grades:
        print(f"Оцінки студентів у групі '{group_name}' з предмета '{discipline_name}':")
        for grade in grades:
            print(f"Студент: {grade.student.fullname}, Оцінка: {grade.grade}")
    else:
        print(f"Не вдалося знайти оцінки студентів у групі '{group_name}' з предмета '{discipline_name}'")


def select_8():
    """ Знайти середній бал, який ставить певний викладач зі своїх предметів.
    :return: """

    result = session.query(distinct(Teacher.fullname), func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Discipline) \
        .join(Teacher) \
        .where(Teacher.id == 3).group_by(Teacher.fullname).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_9(student_name):
    """Знайти список курсів, які відвідує певний студент."""

    student = session.query(Student).filter(Student.fullname == student_name).first()

    if student is None:
        print(f"Студента '{student_name}' не знайдено")
        return

    courses = (
        session.query(Discipline)
        .join(Grade)
        .filter(Grade.student_id == student.id)
        .distinct()
        .all()
    )

    if courses:
        print(f"Список курсів, які відвідує студент '{student_name}':")
        for course in courses:
            print(course.name)
    else:
        print(f"Не вдалося знайти курси, які відвідує студент '{student_name}'")


def select_10(student_name, teacher_name):
    """Список курсів, які певному студенту читає певний викладач."""

    student = session.query(Student).filter(Student.fullname == student_name).first()

    if student is None:
        print(f"Студента '{student_name}' не знайдено")
        return

    teacher = session.query(Teacher).filter(Teacher.fullname == teacher_name).first()

    if teacher is None:
        print(f"Викладача '{teacher_name}' не знайдено")
        return

    courses = (
        session.query(Discipline)
        .join(Grade)
        .filter(Grade.student_id == student.id, Discipline.teacher_id == teacher.id)
        .distinct()
        .all()
    )

    if courses:
        print(f"Список курсів, які студент '{student_name}' відвідує від викладача '{teacher_name}':")
        for course in courses:
            print(course.name)
    else:
        print(f"Не вдалося знайти курси, які студент '{student_name}' відвідує від викладача '{teacher_name}'")


if __name__ == "__main__":
    # print(select_1())
    # print(select_2("Математика"))
    # print(select_3(5))
    # print(select_4())
    # print(select_5("Bryan Sharp"))
    # print(select_6(group_name="GoIT12"))
    # print(select_7("GoIT12", "Математика"))
    # print(select_8())
    # print(select_9("Mark Farmer"))
    # print(select_10("John Doe", "Jane Smith"))
