package pt.isel.ngspipes.engine_core.utils;

import com.jcraft.jsch.*;

import java.io.*;
import java.util.Properties;
import java.util.Vector;

public class SSHUtils {

    public static ChannelSftp getChannelSftp(SSHConfig sshConfig) throws JSchException {
        Session session = getSessionByConfig(sshConfig);
        ChannelSftp channelSftp;

        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        session.connect();

        channelSftp = (ChannelSftp) session.openChannel("sftp");
        channelSftp.connect();
        return channelSftp;
    }

    public static void upload(String dest, String source, ChannelSftp sftp) throws SftpException, FileNotFoundException, UnsupportedEncodingException {
        File file = new File(source);
        createIfNotExist(dest, sftp);
        sftp.cd(dest);
        System.out.println("directory: " + dest);
        if(file.isFile()){
            InputStream ins = new FileInputStream(file);
            sftp.put(ins, new String(file.getName().getBytes(),"UTF-8"));
        } else{
            File[] files = file.listFiles();
            assert files != null;
            for (File file2 : files) {
                String dir = file2.getAbsolutePath();
                if(file2.isDirectory()){
                    String str = dir.substring(dir.lastIndexOf(File.separatorChar));
                    dest = dest + str;
                }
                System.out.println("directory is :" + dest);
                upload(dest, source + dir, sftp);
            }
        }
    }

    public static void copy(String source, String dest, String inputName, SSHConfig config) throws JSchException, InterruptedException, SftpException {
        ChannelSftp sftp = null;
        try {
            sftp = SSHUtils.getChannelSftp(config);
            createIfNotExist(dest, sftp);
            String cpyCmd = "cp -R " + source + inputName + " " + dest;
            Session session = sftp.getSession();
            ChannelExec channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(cpyCmd);
            channel.connect();
            while(channel.isConnected()) {
                Thread.sleep(20);
            }
            int status = channel.getExitStatus();
            if(status != 0)
                throw new JSchException("Error copying input file " + inputName);

        } finally {
            if(sftp != null) {
                sftp.disconnect();
            }
        }
    }

    public static void createIfNotExist(String folderPath, ChannelSftp sftp) throws SftpException {
        try {
            Vector content = sftp.ls(folderPath);
            if(content == null){
                sftp.mkdir(folderPath);
                System.out.println("mkdir:" + folderPath);
            }
        } catch (SftpException e) {
            sftp.mkdir(folderPath);
        }
    }

    private static Session getSessionByConfig(SSHConfig sshConfig) throws JSchException {
        Session session;
        if (sshConfig.getKeyPath() == null || sshConfig.getKeyPath().isEmpty())
            session = getSession(sshConfig);
        else
            session = getSessionWithKey(sshConfig);
        return session;
    }

    private static Session getSession(SSHConfig config) throws JSchException {
        JSch jsch = new JSch();
        Session session = jsch.getSession(config.getUser(), config.getHost(), config.getPort());
        session.setPassword(config.getPassword());
        session.setConfig("StrictHostKeyChecking", "no"); // new
        return session;
    }

    private static Session getSessionWithKey(SSHConfig config) throws JSchException {
        JSch jsch = new JSch();
        jsch.addIdentity(config.getKeyPath(), config.getPassword());
        Session session = jsch.getSession(config.getUser(), config.getHost(), config.getPort());
        session.setPassword(config.getPassword());
        session.setConfig("StrictHostKeyChecking", "no"); // new
        return session;
    }

}
