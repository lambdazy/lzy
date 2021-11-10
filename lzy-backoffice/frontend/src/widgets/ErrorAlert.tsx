import { createContext } from "react";
import { useState } from "react";
import { useContext } from "react";
import { FC } from "react";
import { Alert, Button } from "react-bootstrap";

export interface AlertPatrams {
  show: boolean;
  text: string | undefined;
  header: string | undefined;
  onClose: (() => void) | undefined;
}

export interface AlertContext {
  params: AlertPatrams;
  show: (
    text: string,
    header: string,
    onClose: (() => void) | undefined
  ) => void;
  close: () => void;
}

const alertContext = createContext<AlertContext>({
  params: {
    show: false,
    text: undefined,
    header: undefined,
    onClose: undefined,
  },
  show: () => {},
  close: () => {},
});

export function useAlert(): AlertContext {
  return useContext(alertContext);
}

export function useProvideAlert(): AlertContext {
  const [showState, setShowState] = useState<AlertPatrams>({
    show: false,
    text: undefined,
    header: undefined,
    onClose: undefined,
  });
  const show = (
    text: string,
    header: string,
    onClose: (() => void) | undefined
  ) => {
    setShowState({
      show: true,
      text,
      header,
      onClose,
    });
  };
  const close = () => {
    setShowState({
      show: false,
      text: undefined,
      header: undefined,
      onClose: undefined,
    });
  };
  return { params: showState, show, close };
}

export function ProvideAlert(props: { children: any }) {
  const auth = useProvideAlert();
  return (
    <alertContext.Provider value={auth}>{props.children}</alertContext.Provider>
  );
}

export const ErrorAlert: FC<{}> = () => {
  let alert = useAlert();
  return (
    <Alert
      show={alert.params.show}
      onClose={() => {
        alert.close();
        if (alert.params.onClose !== undefined) alert.params.onClose();
      }}
      variant="danger"
      dismissible
    >
      <Alert.Heading>{alert.params.header}</Alert.Heading>
      <p>{alert.params.text}</p>
    </Alert>
  );
};
