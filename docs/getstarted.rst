.. _getstart-page:

*******************
Get started
*******************

.. _getstart-requirement:

Requirements
============

`Nextflow` can be used on any POSIX compatible system (Linux, Solaris, OS X, etc).
It requires BASH and `Java 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_ or higher to be installed.

Windows systems may be supported using a POSIX compatibility layer like `Cygwin <http://www.cygwin.com>`_ (unverified) or
in alternative installing it into a Linux VM using a virtualization software like `VirtualBox <http://www.virtualbox.org>`_
or `VMware <http://www.vmware.com/>`_.

.. _getstart-install:

Installation
============

`Nextflow` is distributed as a self-contained executable package, this means that it does not require any special installation procedure.

It only needs two easy steps:

#.  Download the executable package by copying and pasting the following command in your terminal
    window: ``wget -qO- get.nextflow.io | bash``.
    It will create the ``nextflow`` main executable file in the current directory.

#.  Optionally, move the ``nextflow`` file in a directory accessible by your ``$PATH`` variable
    (this is only required to avoid to remember and type the Nextflow full path each time you need to run it).

.. tip:: In the case you don't have ``wget`` installed you can use the ``curl`` utility instead by entering
   the following command: ``curl -fsSL get.nextflow.io | bash``


.. _getstart-first:

Your first script
==================

By using your favourite text editor copy the following example, and save it to a file named ``tutorial.nf`` ::

    #!/usr/bin/env nextflow

    params.str = 'Hello world!'

    process splitLetters {

        output:
        file 'chunk_*' into letters mode flatten

        """
        printf '${params.str}' | split -b 6 - chunk_
        """
    }


    process convertToUpper {

        input:
        file x from letters

        output:
        stdout result

        """
        cat $x | tr '[a-z]' '[A-Z]'
        """
    }

    result.subscribe {
        println it.trim()
    }


This script is made up of two processes, the first splits a string in file chunks containing 6 characters,
the following process receives these files and transform the content to uppercase letters.
The resulting strings are emitted on the ``result`` channel and finally the output is printed out by the
``subscribe`` operator.



Execute the script by entering the following command in your terminal::

   nextflow run tutorial.nf

It will output something similar to the text shown below::

    N E X T F L O W  ~  version 0.9.0
    [warm up] executor > local
    [22/7548fa] Submitted process > splitLetters (1)
    [e2/008ee9] Submitted process > convertToUpper (1)
    [1e/165130] Submitted process > convertToUpper (2)
    HELLO
    WORLD!


You can see that the first process is execute once, while the second two times. Finally the result string is printed.

It's worth noting that the process ``convertToUpper`` is executed in parallel, so there's no guarantee that the instance
processing the first split (the chunk `Hello`) is executed before before the one processing the second split (the chunk `world!`).

Thus, it is perfectly possible that you get the final result printed out in a different order::

    WORLD!
    HELLO



.. tip:: The hexadecimal numbers, like ``22/7548fa``, identify the unique process execution. These numbers are
  also the prefix of the directories where each process is executed. You can inspect the files produced by them
  changing to the directory ``$PWD/work`` and using that numbers to find the process specific execution path.

.. _getstart-resume:

Modify and resume
-----------------

Nextflow keep track of all the processes executed in your pipeline. If you modify some parts of your script,
only the processes that are actually changed will be re-executed. The execution of the processes that are not changed
will be skipped and the cached result used instead.

This helps a lot when testing or modifying part of your pipeline without having to re-execute it from scratch.

For the sake of this tutorial modify the ``convertToUpper`` process in the previous example, by replacing the
process script with the string ``rev $x``, so that the process will look like as the following one::

    process convertToUpper {

        input:
        file x from letters

        output:
        stdout result

        """
        rev $x
        """
    }

Then save the file with the same name, and execute it by adding the ``-resume`` option to the command line::

    nextflow run tutorial.nf -resume


It will print an output similar to the following one::

    N E X T F L O W  ~  version 0.9.0
    [warm up] executor > local
    [22/7548fa] Cached process > splitLetters (1)
    [d0/7b79a3] Submitted process > convertToUpper (1)
    [b0/c99ef9] Submitted process > convertToUpper (2)
    olleH
    !dlrow


You will see that the execution of the process ``splitLetters`` is actually skipped (the process ID is the same), and
its results are retried from the cache. Instead the second one is executed as expected, printing the reversed strings.


.. tip:: The pipeline results are cached by default in the directory ``$PWD/work``. Depending your script this folder
  can take of lot of disk space. If your are sure you won't resume your pipeline execution, clean this folder periodically.

.. _getstart-params:

Pipeline parameters
--------------------

Pipeline parameters are simply declared by pre-pending to a variable name the prefix ``params`` separated by dot character.
Their value can be specified on the command line by prefixing the parameter name with a double `dash` character, i.e. ``--paramName``

For the sake of this tutorial you can try to execute the previous example specifying a different input
string parameter, as shown below::

  nextflow run tutorial.nf --str 'Hola mundo'


The string specified on the command line will override the default value of the parameter. The output
will look like as the following::

    N E X T F L O W  ~  version 0.7.0
    [warm up] executor > local
    [6d/54ab39] Submitted process > splitLetters (1)
    [a1/88716d] Submitted process > convertToUpper (2)
    [7d/3561b6] Submitted process > convertToUpper (1)
    odnu
    m aloH




